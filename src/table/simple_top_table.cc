//
// Created by leipeng on 2026-07-26
//
// SimpleTopTable — short-lived L1 mmap SST (especially memtable_as_log_index)
//
// Naming: "Top" is a double entendre — Top Level (L1) and Topling.
//
// Motivation (why minimize build / sequential-read cost, skip compression)
// - Primary target: L1 SST. Especially with memtable_as_log_index there is no
//   separate L0 flush — compact-to-L1 folds "L0 flush + compact to L1" into one
//   step (input already ordered). Builder and reader cost, especially sequential
//   scan of the new L1 file, then dominates end-to-end latency; the cheaper they
//   are, the larger the win. Memory / on-disk size is secondary → no compression.
// - With dcompact, that to-L1 compact often runs on a worker; lowering its build
//   and later sequential-merge read cost matters even more.
//
// Implications
// - Prefer simple layout over archival density (short SST lifetime)
// - Fast build: no sort, no compression, no prefix stripping
// - Fast sequential scan: adjacent [ukey|tag|value], mmap + forward iteration
// - Fast file write: Topling WriteMethod (default kToplingFileWrite)
// - Full 8-byte tags; multi-version / all IsValueType
//
// Trade-offs
// - Point Get/Seek: mmap binary search — OK, slower than bloom/trie/CSPP
// - No compression / no prefix / no KV split pools → more space, simpler code
// - Ordered Add() required; unordered input undefined (checked only if debugLevel>=2)
//
#if defined(__clang__)
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#endif
#include <rocksdb/table.h>
#include <monitoring/iostats_context_imp.h>
#include <topling/side_plugin_factory.h>
#include <topling/builtin_table_factory.h>

#include <logging/logging.h>
#include <table/meta_blocks.h>
#include <table/get_context.h>
#include <db/dbformat.h>

#include "top_table_reader.h"
#include "top_table_builder.h"
#include "top_fast_table.h"

#include <terark/bitmap.hpp>
#include <terark/int_vector.hpp> // for compute_uintbits
#include <terark/io/FileStream.hpp>
#include <terark/io/MemMapStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/num_to_str.hpp>
#include <terark/util/atomic.hpp>

namespace rocksdb {

using namespace terark;

const uint64_t kSimpleTopTableMagic = 0x54706f54706d6953ULL; // SimpTopT
const std::string kMetaName = "StrVecIndex";

class SimpleTopTableFactory : public TableFactory {
public:
  SimpleTopTableFactory(const json& js, const SidePluginRepo& repo) {
    Update({}, js, repo);
  }
  ~SimpleTopTableFactory() override = default;

  void Update(const json&, const json&, const SidePluginRepo&);
  std::string ToString(const json& dump_options, const SidePluginRepo&) const;

  const char* Name() const override { return "SimpleTopTable"; }

  using TableFactory::NewTableReader;
  Status NewTableReader(const ReadOptions&,
                        const TableReaderOptions&,
                        std::unique_ptr<RandomAccessFileReader>&&,
                        uint64_t file_size,
                        std::unique_ptr<TableReader>*,
                        bool prefetch_index_and_filter_in_cache) const override;

  TableBuilder* NewTableBuilder(const TableBuilderOptions&,
                                WritableFileWriter*) const override;

  std::string GetPrintableOptions() const final;
  Status ValidateOptions(const DBOptions&, const ColumnFamilyOptions&) const final;

  bool IsDeleteRangeSupported() const override { return true; }
  bool SupportAutoSort() const final { return false; }

  WriteMethod writeMethod = WriteMethod::kToplingFileWrite;
  uint32_t fileWriteBufferSize = 8 * 1024;
  // Default true: keep short-lived L1 moving (esp. memtable_as_log_index /
  // dcompact path in file header). Avoid parking these SSTs in place.
  bool forceNeedCompact = true;
  bool collectProperties = true;
  int debugLevel = 0;

  mutable long long start_time_point = 0;
  mutable long long build_time_duration = 0;
  mutable size_t num_writers = 0;
  mutable size_t num_readers = 0;
  mutable size_t sum_user_key_len = 0;
  mutable size_t sum_user_key_cnt = 0;
  mutable size_t sum_value_len = 0;
  mutable size_t sum_index_len = 0;

  struct MetaInfo {
    uint32_t version;
    uint8_t  offset_bits;   // 0..64
    uint8_t  keylen_bits;   // 0..64
    uint16_t padding;
    uint32_t min_ukey_len;
    uint32_t max_ukey_len;
    uint64_t indexed_num;
    uint64_t record_pool_size;
    uint64_t index_bytes;
    uint64_t min_seqno;
    uint64_t max_seqno;
    uint64_t reserved[8];  // 64 bytes, must be zero; for future fields
    Slice Memory() const {
      return {(const char*)this, sizeof(MetaInfo)};
    }
  };
};
using MetaInfo = SimpleTopTableFactory::MetaInfo;
static_assert(sizeof(MetaInfo) == 120);
static_assert(sizeof(MetaInfo) % 8 == 0);
static_assert(offsetof(MetaInfo, record_pool_size) % 8 == 0);
static_assert(offsetof(MetaInfo, min_seqno) == 40);
static_assert(offsetof(MetaInfo, reserved) == 56);
static_assert(sizeof(((MetaInfo*)nullptr)->reserved) == 64);

/////////////////////////////////////////////////////////////////////////////
// Builder
/////////////////////////////////////////////////////////////////////////////

class SimpleTopTableBuilder : public TopTableBuilderBase {
public:
  SimpleTopTableBuilder(const SimpleTopTableFactory*, const TableBuilderOptions&,
                        WritableFileWriter* file);
  void Add(const Slice& key, const Slice& value) final;
  Status Finish() final;
  void Abandon() final;
  uint64_t EstimatedFileSize() const final;
  void DoWriteAppend(const void* data, size_t size);
  void ToplingFlushBuffer();
  bool NeedCompact() const final;
  void DebugCheckTable();

  const SimpleTopTableFactory* table_factory_;
  WriteMethod writeMethod_;
  bool collectProperties_;
  bool forceNeedCompact_;
  OsFileStream fstream_;
  OutputBuffer fobuf_;
  MemMapStream fmap_;
  valvec<size_t> kv_offsets_;
  valvec<uint32_t> keylens_; // internal key len = ukey + 8
  uint32_t min_key_len_ = UINT32_MAX, max_key_len_ = 0; // ikey len
  uint32_t min_val_len_ = UINT32_MAX, max_val_len_ = 0;
  SequenceNumber min_seqno_ = kMaxSequenceNumber, max_seqno_ = 0;
  long long t0 = 0;
  std::vector<std::pair<std::string, std::string>> kv_debug_;
};

uint64_t SimpleTopTableBuilder::EstimatedFileSize() const {
  // During Add only the KV pool is on file (offset_). Finish appends a
  // bit-packed index (if any) then meta/footer → final FileSize > estimate.
  // Dual-fixed: kv_offsets_/keylens_ stay empty → estimate == offset_.
  // Else: same bit-widths as Finish, convert bits→bytes (/8, trunc) and omit
  // the guard offset so we stay strictly below final index_bytes + meta.
  return offset_
      + UintVecMin0::compute_uintbits(offset_) * kv_offsets_.size() / 8
      + UintVecMin0::compute_uintbits(max_key_len_) * keylens_.size() / 8;
}

SimpleTopTableBuilder::SimpleTopTableBuilder(
    const SimpleTopTableFactory* table_factory,
    const TableBuilderOptions& tbo,
    WritableFileWriter* file)
  : TopTableBuilderBase(tbo, file)
  , table_factory_(table_factory)
{
  properties_.compression_name = "SimpTop";
  debugLevel_ = (signed char)table_factory->debugLevel;
  writeMethod_ = table_factory->writeMethod;
  collectProperties_ = table_factory->collectProperties;
  forceNeedCompact_ = table_factory->forceNeedCompact;
  if (ioptions_.file_checksum_gen_factory) {
    if (WriteMethod::kRocksdbNative != writeMethod_) {
      WARN(ioptions_.info_log,
           "file_checksum_gen_factory is not null, use kRocksdbNative");
      writeMethod_ = WriteMethod::kRocksdbNative;
    }
  }
  if (WriteMethod::kToplingMmapWrite == writeMethod_) {
    TERARK_VERIFY(nullptr == ioptions_.file_checksum_gen_factory);
    auto fd = file->writable_file()->FileDescriptor();
    auto& fname = file->file_name();
    try {
      fmap_.dopen(fd, tbo.target_file_size, fname, O_RDWR);
    } catch (const std::exception& ex) {
      writeMethod_ = WriteMethod::kRocksdbNative;
      ROCKS_LOG_WARN(ioptions_.info_log,
          "fmap_.dopen(%s) = %s, set as kRocksdbNative",
          fname.c_str(), ex.what());
    }
  } else if (WriteMethod::kToplingFileWrite == writeMethod_) {
    TERARK_VERIFY(nullptr == ioptions_.file_checksum_gen_factory);
    int fd = (int)file_->writable_file()->FileDescriptor();
    TERARK_VERIFY_GE(fd, 0);
    fstream_.attach(fd);
    fobuf_.attach(&fstream_);
    fobuf_.initbuf(table_factory->fileWriteBufferSize);
  }
  t0 = g_pf.now();
}

void SimpleTopTableBuilder::DoWriteAppend(const void* data, size_t size) {
  switch (writeMethod_) {
  default:
    TERARK_DIE("memory corruption: writeMethod = %d", int(writeMethod_));
    break;
  case WriteMethod::kToplingFileWrite:
    fobuf_.ensureWrite(data, size);
    break;
  case WriteMethod::kToplingMmapWrite:
    fmap_.ensureWrite(data, size);
    break;
  case WriteMethod::kRocksdbNative: {
    IOStatus s = file_->Append(Slice((const char*)data, size));
    if (!s.ok()) {
      throw s; // NOLINT
    }
    break; }
  }
  offset_ += size;
}

void SimpleTopTableBuilder::ToplingFlushBuffer() {
  if (WriteMethod::kToplingMmapWrite == writeMethod_) {
    TERARK_VERIFY_EQ(fmap_.tell(), offset_);
    fmap_.close();
    auto fd = (int)file_->writable_file()->FileDescriptor();
    lseek(fd, offset_, SEEK_SET);
    file_->SetFileSize(offset_);
    file_->writable_file()->SetFileSize(offset_);
    IOSTATS_ADD(bytes_written, offset_);
  } else if (WriteMethod::kToplingFileWrite == writeMethod_) {
    fobuf_.flush_buffer();
    fstream_.detach();
    file_->SetFileSize(offset_);
    file_->writable_file()->SetFileSize(offset_);
    IOSTATS_ADD(bytes_written, offset_);
  } else {
    TERARK_VERIFY(WriteMethod::kRocksdbNative == writeMethod_);
    file_->Flush();
  }
}

void SimpleTopTableBuilder::Add(const Slice& key, const Slice& value) try {
  TERARK_ASSERT_GE(key.size(), 8);
  const uint64_t seqvt = DecodeFixed64(key.data() + key.size() - 8);
  const auto vt = ValueType(seqvt & 255u);
  TERARK_ASSERT_EZ((vt & 0x80u));
  if (IsValueType(vt)) {
    const SequenceNumber seqno = seqvt >> 8;
    minimize(min_seqno_, seqno);
    maximize(max_seqno_, seqno);
    if (vt == kTypeDeletion || vt == kTypeSingleDeletion) {
      properties_.num_deletions++;
    } else if (vt == kTypeMerge) {
      properties_.num_merge_operands++;
    }
    if (debugLevel_ >= 2 && !kv_debug_.empty()) {
      Slice prev = kv_debug_.back().first;
      if (isReverseBytewiseOrder_) {
        TERARK_VERIFY(RevBytewiseCompareInternalKey()(prev, key));
      } else {
        TERARK_VERIFY(BytewiseCompareInternalKey()(prev, key));
      }
    }
    const size_t kv_off = size_t(offset_);
    // kv_offsets_/keylens_ lazy: dual-fixed needs neither; single-varlen needs
    // offsets; dual-varlen needs both. Backfill when first required.
    const bool had_entries = properties_.num_entries > 0;
    const bool key_fixed_before =
        !had_entries || (min_key_len_ == max_key_len_);
    const bool val_fixed_before =
        !had_entries || (min_val_len_ == max_val_len_);
    const uint32_t prev_key_lo = min_key_len_;
    const uint32_t prev_val_lo = min_val_len_;
    DoWriteAppend(key.data(), key.size());
    DoWriteAppend(value.data(), value.size());
    minimize(min_key_len_, key.size());
    maximize(max_key_len_, key.size());
    minimize(min_val_len_, value.size());
    maximize(max_val_len_, value.size());
    const bool need_index =
        (min_key_len_ != max_key_len_) || (min_val_len_ != max_val_len_);
    if (need_index) {
      if (kv_offsets_.empty()) {
        // previous entries were dual-fixed — reconstruct arithmetic offsets
        TERARK_VERIFY(key_fixed_before && val_fixed_before);
        const size_t stride = size_t(prev_key_lo) + size_t(prev_val_lo);
        TERARK_VERIFY_EQ(kv_off, properties_.num_entries * stride);
        kv_offsets_.reserve(std::max(size_t(4096), properties_.num_entries + 1));
        kv_offsets_.resize_no_init(properties_.num_entries);
        for (size_t j = 0; j < properties_.num_entries; j++) {
          kv_offsets_[j] = j * stride;
        }
      }
      kv_offsets_.push_back(kv_off);
      if (min_key_len_ != max_key_len_ && min_val_len_ != max_val_len_) {
        if (keylens_.empty()) {
          keylens_.reserve(std::max(size_t(4096), properties_.num_entries + 1));
          if (key_fixed_before) {
            keylens_.resize_fill(properties_.num_entries, prev_key_lo);
          } else {
            TERARK_VERIFY(val_fixed_before);
            TERARK_VERIFY_EQ(kv_offsets_.size(), properties_.num_entries + 1);
            keylens_.resize_no_init(properties_.num_entries);
            for (size_t j = 0; j < properties_.num_entries; j++) {
              keylens_[j] = uint32_t(kv_offsets_[j + 1] - kv_offsets_[j] -
                                     prev_val_lo);
            }
          }
        }
        keylens_.push_back(uint32_t(key.size()));
      }
    }
    properties_.num_entries++;
    properties_.raw_key_size += key.size();
    properties_.raw_value_size += value.size();
    if (collectProperties_) {
      NotifyCollectTableCollectorsOnAdd(key, value, kv_off, collectors_,
                                        ioptions_.info_log.get());
    }
    if (UNLIKELY(debugLevel_ >= 2)) {
      kv_debug_.emplace_back(key.ToString(), value.ToString());
    }
  } else if (vt == kTypeRangeDeletion) {
    range_del_block_.Add(key, value);
    properties_.num_range_deletions++;
  } else {
    const char* ename = enum_name(vt).data();
    TERARK_DIE("unexpected ValueType = %s(%d)", ename, vt);
  }
}
catch (const IOStatus& s) {
  TERARK_VERIFY(!s.ok());
  WARN_EXCEPT(ioptions_.info_log, "%s: IOStatus: %s", BOOST_CURRENT_FUNCTION,
              s.ToString().c_str());
  io_status_ = s;
}
catch (const Status& s) {
  TERARK_VERIFY(!s.ok());
  WARN_EXCEPT(ioptions_.info_log, "%s: Status: %s", BOOST_CURRENT_FUNCTION,
              s.ToString().c_str());
  status_ = s;
}
catch (const std::exception& ex) {
  WARN_EXCEPT(ioptions_.info_log, "%s: std::exception: %s",
              BOOST_CURRENT_FUNCTION, ex.what());
  status_ = Status::Corruption(ROCKSDB_FUNC, ex.what());
}

bool SimpleTopTableBuilder::NeedCompact() const {
  if (forceNeedCompact_) {
    return true;
  }
  return TopTableBuilderBase::NeedCompact();
}

Status SimpleTopTableBuilder::Finish() try {
  if (0 == properties_.num_entries) {
    TERARK_VERIFY_EQ(offset_, 0);
    ToplingFlushBuffer();
    FinishAsEmptyTable();
    return Status::OK();
  }
  using namespace std::placeholders;
  auto writeAppend =
      std::bind(&SimpleTopTableBuilder::DoWriteAppend, this, _1, _2);

  const size_t n = size_t(properties_.num_entries);
  const size_t pool_size = size_t(offset_); // record pool already written in Add

  properties_.tag_size = 8 * n;
  properties_.num_data_blocks = 1;
  properties_.data_size = pool_size;

  const bool key_fixed = (min_key_len_ == max_key_len_);
  const bool val_fixed = (min_val_len_ == max_val_len_);

  MetaInfo meta{};
  meta.version = 1;
  meta.padding = 0;
  // reserved[8] zeroed by MetaInfo{}
  meta.min_ukey_len = min_key_len_ - 8;
  meta.max_ukey_len = max_key_len_ - 8;
  meta.indexed_num = n;
  meta.record_pool_size = pool_size;
  meta.min_seqno = min_seqno_;
  meta.max_seqno = max_seqno_;

  if (key_fixed && val_fixed) {
    TERARK_VERIFY(kv_offsets_.empty());
    TERARK_VERIFY(keylens_.empty());
    properties_.fixed_key_len = max_key_len_;
    properties_.fixed_value_len = max_val_len_;
    meta.offset_bits = 0;
    meta.keylen_bits = 0;
    meta.index_bytes = 0;
  } else {
    TERARK_VERIFY_EQ(kv_offsets_.size(), n);
    Padzero<64>(writeAppend, offset_);
    const size_t offset_bits = UintVecMin0::compute_uintbits(pool_size);
    TERARK_VERIFY_LE(offset_bits, sizeof(size_t) * 8);
    febitvec bits;
    if (key_fixed || val_fixed) {
      // single-varlen: offsets only
      TERARK_VERIFY(keylens_.empty());
      // fixed_value_len: >=0 fixed (0 = fixed-empty); uint64_t(-1) = variable
      if (key_fixed) {
        properties_.fixed_key_len = max_key_len_;
        properties_.fixed_value_len = uint64_t(-1);
      } else {
        properties_.fixed_key_len = 0;
        properties_.fixed_value_len = max_val_len_;
      }
      meta.offset_bits = uint8_t(offset_bits);
      meta.keylen_bits = 0;
      bits.resize_no_init((n + 1) * offset_bits);
      bits.fill(false);
      for (size_t i = 0; i < n; i++) {
        bits.set_uint(i * offset_bits, offset_bits, (ullong)kv_offsets_[i]);
      }
      bits.set_uint(n * offset_bits, offset_bits, (ullong)pool_size);
    } else {
      // dual-varlen
      TERARK_VERIFY_EQ(keylens_.size(), n);
      properties_.fixed_key_len = 0;
      properties_.fixed_value_len = uint64_t(-1);
      const size_t keylen_bits = UintVecMin0::compute_uintbits(max_key_len_);
      TERARK_VERIFY_GT(keylen_bits, 0u);
      TERARK_VERIFY_LE(keylen_bits, sizeof(size_t) * 8);
      meta.offset_bits = uint8_t(offset_bits);
      meta.keylen_bits = uint8_t(keylen_bits);
      const size_t stride = offset_bits + keylen_bits;
      bits.resize_no_init(n * stride + offset_bits);
      bits.fill(false);
      for (size_t i = 0; i < n; i++) {
        bits.set_uint(i * stride, offset_bits, (ullong)kv_offsets_[i]);
        bits.set_uint(i * stride + offset_bits, keylen_bits, (ullong)keylens_[i]);
      }
      bits.set_uint(n * stride, offset_bits, (ullong)pool_size);
    }
    meta.index_bytes = bits.mem_size();
    DoWriteAppend(bits.data(), bits.mem_size());
    properties_.index_size = meta.index_bytes;
  }

  ToplingFlushBuffer();
  WriteMeta(kSimpleTopTableMagic, {{kMetaName, WriteBlock(meta.Memory(), file_, &offset_)}});

  auto fac = table_factory_;
  auto ukey_len = properties_.raw_key_size - 8 * n;
  long long t1 = g_pf.now();
  auto td = g_pf.us(t1 - t0);
  // entry count (not unique ukey); SimpleTop does not collapse versions at build
  as_atomic(fac->sum_user_key_cnt).fetch_add(n, std::memory_order_relaxed);
  as_atomic(fac->sum_user_key_len).fetch_add(ukey_len, std::memory_order_relaxed);
  as_atomic(fac->sum_value_len).fetch_add(properties_.raw_value_size, std::memory_order_relaxed);
  as_atomic(fac->sum_index_len).fetch_add(properties_.index_size, std::memory_order_relaxed);
  as_atomic(fac->build_time_duration).fetch_add(td, std::memory_order_relaxed);

  closed_ = true;
  if (debugLevel_ >= 2) {
    file_->Flush();
    DebugCheckTable();
  }
  return Status::OK();
}
catch (const IOStatus& s) {
  io_status_ = s;
  closed_ = true;
  return Status::IOError(ROCKSDB_FUNC, s.ToString());
}
catch (const Status& s) {
  status_ = s;
  closed_ = true;
  return s;
}
catch (const std::exception& ex) {
  status_ = Status::Corruption(ROCKSDB_FUNC, ex.what());
  closed_ = true;
  return status_;
}

void SimpleTopTableBuilder::Abandon() {
  fobuf_.resetbuf();
  ToplingFlushBuffer();
  closed_ = true;
}

std::unique_ptr<TableReader>
OpenSST(const std::string& fname, uint64_t fsize, const ImmutableOptions&);

void SimpleTopTableBuilder::DebugCheckTable() {
  auto tr_uptr = OpenSST(file_->file_name(), offset_, ioptions_);
  auto tr = tr_uptr.get();
  auto new_iter = [&]() {
    ReadOptions ro;
    return tr->NewIterator(ro, nullptr, nullptr, false, kUserIterator);
  };
  auto it = new_iter();
  it->SeekToFirst();
  size_t idx = 0, num = kv_debug_.size();
  while (it->Valid()) {
    TERARK_VERIFY_LT(idx, num);
    TERARK_VERIFY_S(kv_debug_[idx].first == it->key(), "%s <=> %s",
                    kv_debug_[idx].first, it->key().ToString(true));
    TERARK_VERIFY_S(kv_debug_[idx].second == it->value(), "%s <=> %s",
                    kv_debug_[idx].second, it->value().ToString(true));
    it->Next();
    idx++;
  }
  TERARK_VERIFY_EQ(idx, num);
  // reverse iterate
  it->SeekToLast();
  idx = num;
  while (it->Valid()) {
    idx--;
    TERARK_VERIFY_LT(idx, num);
    TERARK_VERIFY_S(kv_debug_[idx].first == it->key(), "%s <=> %s",
                    kv_debug_[idx].first, it->key().ToString(true));
    TERARK_VERIFY_S(kv_debug_[idx].second == it->value(), "%s <=> %s",
                    kv_debug_[idx].second, it->value().ToString(true));
    it->Prev();
  }
  TERARK_VERIFY_EQ(idx, 0u);
  // Seek each internal key
  for (idx = 0; idx < num; idx++) {
    Slice ikey = kv_debug_[idx].first;
    it->Seek(ikey);
    TERARK_VERIFY_S(it->Valid(), "%zd/%zd : %s", idx, num, ikey.ToString(true));
    TERARK_VERIFY_S(kv_debug_[idx].first == it->key(), "%s <=> %s",
                    kv_debug_[idx].first, it->key().ToString(true));
    TERARK_VERIFY_S(kv_debug_[idx].second == it->value(), "%s <=> %s",
                    kv_debug_[idx].second, it->value().ToString(true));
  }
  // Get: resolve expected value from file order (first seq <= target in equal-ukey)
  {
    ReadOptions ro; // keep pinning false — ~ReadOptions asserts if left true
    for (idx = 0; idx < num; idx++) {
      Slice ikey = kv_debug_[idx].first;
      TERARK_VERIFY_GE(ikey.size(), 8u);
      ParsedInternalKey target(ikey);
      size_t lo = idx, hi = idx + 1;
      while (lo > 0) {
        Slice prev = kv_debug_[lo - 1].first;
        if (Slice(prev.data(), prev.size() - 8) != target.user_key) break;
        lo--;
      }
      while (hi < num) {
        Slice next = kv_debug_[hi].first;
        if (Slice(next.data(), next.size() - 8) != target.user_key) break;
        hi++;
      }
      size_t expect = SIZE_MAX;
      for (size_t j = lo; j < hi; j++) {
        ParsedInternalKey pj(kv_debug_[j].first);
        if (pj.sequence <= target.sequence) {
          expect = j;
          break;
        }
      }
      TERARK_VERIFY_LT(expect, hi);
      PinnableSlice pval;
      GetContext gctx(ioptions_.user_comparator, nullptr, nullptr, nullptr,
                      GetContext::kNotFound, target.user_key, &pval, nullptr,
                      nullptr, nullptr, true, nullptr, nullptr);
      Status gs = tr->Get(ro, ikey, &gctx, nullptr, true);
      TERARK_VERIFY_S(gs.ok(), "%s", gs.ToString());
      ParsedInternalKey expk(kv_debug_[expect].first);
      if (expk.type == kTypeValue || expk.type == kTypeBlobIndex) {
        TERARK_VERIFY_EQ(gctx.State(), GetContext::kFound);
        TERARK_VERIFY_S(pval == Slice(kv_debug_[expect].second), "%s <=> %s",
                        pval.ToString(true),
                        Slice(kv_debug_[expect].second).ToString(true));
      } else if (expk.type == kTypeDeletion || expk.type == kTypeSingleDeletion) {
        TERARK_VERIFY_EQ(gctx.State(), GetContext::kDeleted);
      }
    }
  }
  delete it;
  STD_INFO("SimpleTopTableBuilder::DebugCheckTable(%s) success!",
           file_->file_name().c_str());
}

/////////////////////////////////////////////////////////////////////////////
// Reader
/////////////////////////////////////////////////////////////////////////////

class SimpleTopTableReader : public TopTableReaderBase {
public:
  explicit SimpleTopTableReader(const SimpleTopTableFactory* f) : factory_(f) {}
  ~SimpleTopTableReader() override;
  void Open(RandomAccessFileReader*, Slice file_data, const TableReaderOptions&);

  InternalIterator*
  NewIterator(const ReadOptions&, const SliceTransform* prefix_extractor,
              Arena* arena, bool skip_filters, TableReaderCaller caller,
              size_t compaction_readahead_size,
              bool allow_unprepared_value) final;

  uint64_t ApproximateOffsetOf(ROCKSDB_8_X_COMMA(const ReadOptions& readopt)
                               const Slice& key, TableReaderCaller) final;
  uint64_t ApproximateSize(ROCKSDB_8_X_COMMA(const ReadOptions& readopt)
                           const Slice&, const Slice&, TableReaderCaller) final;
  Status Get(const ReadOptions& readOptions, const Slice& key,
             GetContext* get_context, const SliceTransform* prefix_extractor,
             bool skip_filters) final;
  // Table content checksum: not used (same as TopFast/SingleFast/VecAutoSort).
  // File-level checksum: when DBOptions::file_checksum_gen_factory is set,
  // Builder forces WriteMethod::kRocksdbNative so WritableFileWriter generates
  // it; DB verifies via VerifyFileChecksums / ingest verify_file_checksum.
  Status VerifyChecksum(const ReadOptions&, TableReaderCaller) final {
    return Status::OK();
  }
  std::string ToWebViewString(const json& dump_options) const final;
  bool IsMyFactory(const TableFactory* fac) const final {
    return fac == factory_;
  }

  // record access
  // kFixedKey / kFixedValue: compile-time layout (fixed_value includes len==0)
  template<bool kFixedKey, bool kFixedValue>
  void RecAtTmpl(size_t i, Slice* ikey, Slice* val) const;
  template<bool kFixedKey, bool kFixedValue>
  Slice UkeyAtTmpl(size_t i) const;
  template<bool kFixedKey, bool kFixedValue, class UkeyCmp>
  size_t LowerBoundUkeyTmpl(Slice ukey, UkeyCmp cmp) const;
  template<bool kFixedKey, bool kFixedValue, class UkeyCmp>
  std::pair<size_t, size_t> EqualRangeUkeyTmpl(Slice ukey, UkeyCmp cmp) const;
  template<bool kFixedKey, bool kFixedValue>
  size_t LowerBoundUkeyLayout(Slice ukey) const;
  template<bool kFixedKey, bool kFixedValue>
  std::pair<size_t, size_t> EqualRangeUkeyLayout(Slice ukey) const;
  size_t LowerBoundUkey(Slice ukey) const;

  size_t record_pool_size_ = 0;
  size_t indexed_num_ = 0;
  size_t offset_bits_ = 0;
  size_t keylen_bits_ = 0;
  size_t record_stride_ = 0; // dual-fixed only
  int fixed_key_len_ = 0;
  int fixed_value_len_ = 0;
  febitvec index_bits_;
  const MetaInfo* sstmeta_ = nullptr; // into file mmap (MmapReadWrapper)
  const SimpleTopTableFactory* factory_;
  template<bool kFixedKey, bool kFixedValue, bool kWithGlobalSeqno> class Iter;
  template<bool kFixedKey, bool kFixedValue>
  InternalIterator* NewIterLayout(Arena* a);
  template<bool kFixedKey, bool kFixedValue, bool kWithGlobalSeqno>
  Status GetTpl(const ReadOptions& ro, const Slice& key,
                   GetContext* get_context);
  static bool NeedGlobalSeqnoRewrite(SequenceNumber gseq) {
    // global_seqno_==0 is equivalent to disable (see LoadCommonPart)
    return gseq != 0;
  }
};

template<bool kFixedKey, bool kFixedValue>
void SimpleTopTableReader::RecAtTmpl(size_t i, Slice* ikey, Slice* val) const {
  TERARK_ASSERT_LT(i, indexed_num_);
  size_t kv_off, keylen, value_len;
  if constexpr (kFixedKey && kFixedValue) {
    kv_off = i * record_stride_;
    keylen = size_t(fixed_key_len_);
    value_len = size_t(fixed_value_len_);
  } else if constexpr (kFixedKey) {
    // key fixed, value variable — single-varlen offsets
    size_t BegEnd[2];
    index_bits_.get2_uints(i * offset_bits_, offset_bits_, BegEnd);
    kv_off = BegEnd[0];
    keylen = size_t(fixed_key_len_);
    value_len = BegEnd[1] - kv_off - keylen;
  } else if constexpr (kFixedValue) {
    // key variable, value fixed — single-varlen offsets
    size_t BegEnd[2];
    index_bits_.get2_uints(i * offset_bits_, offset_bits_, BegEnd);
    kv_off = BegEnd[0];
    value_len = size_t(fixed_value_len_);
    keylen = BegEnd[1] - kv_off - value_len;
  } else {
    // dual-varlen
    const size_t stride = offset_bits_ + keylen_bits_;
    kv_off = index_bits_.get_uint<size_t>(i * stride, offset_bits_);
    keylen = index_bits_.get_uint<size_t>(i * stride + offset_bits_, keylen_bits_);
    size_t next = index_bits_.get_uint<size_t>((i + 1) * stride, offset_bits_);
    value_len = next - kv_off - keylen;
  }
  *ikey = Slice(file_data_.data_ + kv_off, keylen);
  *val = Slice(file_data_.data_ + kv_off + keylen, value_len);
}

template<bool kFixedKey, bool kFixedValue>
Slice SimpleTopTableReader::UkeyAtTmpl(size_t i) const {
  // binary-search hot path: resolve ukey only (skip value bounds)
  TERARK_ASSERT_LT(i, indexed_num_);
  size_t kv_off, keylen;
  if constexpr (kFixedKey && kFixedValue) {
    kv_off = i * record_stride_;
    keylen = size_t(fixed_key_len_);
  } else if constexpr (kFixedKey) {
    // key fixed, value var — only need start offset
    kv_off = index_bits_.get_uint<size_t>(i * offset_bits_, offset_bits_);
    keylen = size_t(fixed_key_len_);
  } else if constexpr (kFixedValue) {
    size_t BegEnd[2];
    index_bits_.get2_uints(i * offset_bits_, offset_bits_, BegEnd);
    kv_off = BegEnd[0];
    keylen = BegEnd[1] - kv_off - size_t(fixed_value_len_);
  } else {
    const size_t stride = offset_bits_ + keylen_bits_;
    kv_off = index_bits_.get_uint<size_t>(i * stride, offset_bits_);
    keylen = index_bits_.get_uint<size_t>(i * stride + offset_bits_, keylen_bits_);
  }
  TERARK_ASSERT_GE(keylen, 8u);
  return Slice(file_data_.data_ + kv_off, keylen - 8);
}

template<bool kFixedKey, bool kFixedValue, class UkeyCmp>
size_t SimpleTopTableReader::LowerBoundUkeyTmpl(Slice ukey, UkeyCmp cmp) const {
  size_t lo = 0, hi = indexed_num_;
  while (lo < hi) {
    size_t mid = (lo + hi) / 2;
    if (cmp(UkeyAtTmpl<kFixedKey, kFixedValue>(mid), ukey) < 0)
      lo = mid + 1;
    else
      hi = mid;
  }
  return lo;
}

template<bool kFixedKey, bool kFixedValue, class UkeyCmp>
std::pair<size_t, size_t>
SimpleTopTableReader::EqualRangeUkeyTmpl(Slice ukey, UkeyCmp cmp) const {
  size_t i = 0, j = indexed_num_;
  while (i < j) {
    size_t mid = (i + j) / 2;
    int c = cmp(UkeyAtTmpl<kFixedKey, kFixedValue>(mid), ukey);
    if (c < 0) {
      i = mid + 1;
    } else if (c > 0) {
      j = mid;
    } else {
      // hit: lower in [i, mid), upper in (mid, j]
      size_t lo = i, hi = mid;
      while (lo < hi) {
        size_t m = (lo + hi) / 2;
        if (cmp(UkeyAtTmpl<kFixedKey, kFixedValue>(m), ukey) < 0)
          lo = m + 1;
        else
          hi = m;
      }
      size_t ulo = mid + 1, uhi = j;
      while (ulo < uhi) {
        size_t m = (ulo + uhi) / 2;
        if (cmp(UkeyAtTmpl<kFixedKey, kFixedValue>(m), ukey) <= 0)
          ulo = m + 1;
        else
          uhi = m;
      }
      return {lo, ulo};
    }
  }
  return {i, i};
}

template<bool kFixedKey, bool kFixedValue>
size_t SimpleTopTableReader::LowerBoundUkeyLayout(Slice ukey) const {
  if (isReverseBytewiseOrder_)
    return LowerBoundUkeyTmpl<kFixedKey, kFixedValue>(
        ukey, ReverseBytewiseCompareUserKeyNoTS());
  else
    return LowerBoundUkeyTmpl<kFixedKey, kFixedValue>(
        ukey, ForwardBytewiseCompareUserKeyNoTS());
}

template<bool kFixedKey, bool kFixedValue>
std::pair<size_t, size_t>
SimpleTopTableReader::EqualRangeUkeyLayout(Slice ukey) const {
  if (isReverseBytewiseOrder_)
    return EqualRangeUkeyTmpl<kFixedKey, kFixedValue>(
        ukey, ReverseBytewiseCompareUserKeyNoTS());
  else
    return EqualRangeUkeyTmpl<kFixedKey, kFixedValue>(
        ukey, ForwardBytewiseCompareUserKeyNoTS());
}

size_t SimpleTopTableReader::LowerBoundUkey(Slice ukey) const {
  const bool fk = fixed_key_len_ > 0;
  const bool fv = fixed_value_len_ >= 0;
  if (fk && fv) return LowerBoundUkeyLayout<true, true>(ukey);
  if (fk) return LowerBoundUkeyLayout<true, false>(ukey);
  if (fv) return LowerBoundUkeyLayout<false, true>(ukey);
  return LowerBoundUkeyLayout<false, false>(ukey);
}

void SimpleTopTableReader::Open(RandomAccessFileReader* file, Slice file_data,
                                const TableReaderOptions& tro) {
  uint64_t file_size = file_data.size_;
  try {
    LoadCommonPart(file, tro, file_data, kSimpleTopTableMagic);
  } catch (const Status&) {
    BlockContents emptyTableBC = ReadMetaBlockE(
        file, file_size, kTopEmptyTableMagicNumber, tro.ioptions,
        kTopEmptyTableKey);
    if (emptyTableBC.data.empty())
      throw Status::Corruption(ROCKSDB_FUNC, "empty EmptyTable meta block");
    INFO(tro.ioptions.info_log,
         "SimpleTopTableReader::Open: %s is EmptyTable, it's ok\n",
         file->file_name().c_str());
    auto t = UniquePtrOf(new TopEmptyTableReader());
    file_.release(); // NOLINT
    t->Open(file, file_data, tro);
    throw t.release(); // NOLINT
  }
  // MmapReadWrapper::Read returns pointers into its mmap (same as file_data_);
  // BlockFetcher then builds a non-owning BlockContents — safe to keep pointer.
  BlockContents indexBlock =
      ReadMetaBlockE(file, file_size, kSimpleTopTableMagic, tro.ioptions,
                     kMetaName);
  if (indexBlock.data.size_ < sizeof(MetaInfo))
    throw Status::Corruption(ROCKSDB_FUNC, "meta block is too small");
  if (indexBlock.own_bytes())
    throw Status::Corruption(ROCKSDB_FUNC, "meta block is not backed by the SST mmap");
  if (indexBlock.data.data_ < file_data_.data_ ||
      indexBlock.data.data_ > file_data_.end() ||
      size_t(file_data_.end() - indexBlock.data.data_) < sizeof(MetaInfo))
    throw Status::Corruption(ROCKSDB_FUNC, "meta block is outside the SST mmap");
  sstmeta_ = (const MetaInfo*)indexBlock.data.data_;
  if (sstmeta_->version != 1)
    throw Status::Corruption(ROCKSDB_FUNC, "unsupported meta version");
  if (sstmeta_->min_seqno > sstmeta_->max_seqno ||
      sstmeta_->max_seqno > kMaxSequenceNumber)
    throw Status::Corruption(ROCKSDB_FUNC, "invalid seqno range");
  for (auto w : sstmeta_->reserved) {
    if (w != 0)
      throw Status::Corruption(ROCKSDB_FUNC, "nonzero reserved meta field");
  }

  indexed_num_ = sstmeta_->indexed_num;
  record_pool_size_ = sstmeta_->record_pool_size;
  offset_bits_ = sstmeta_->offset_bits;
  keylen_bits_ = sstmeta_->keylen_bits;
  const uint64_t fixed_key_len = table_properties_->fixed_key_len;
  const uint64_t fixed_value_len = table_properties_->fixed_value_len;
  if (fixed_key_len > INT_MAX || (fixed_value_len != uint64_t(-1) && fixed_value_len > INT_MAX))
    throw Status::Corruption(ROCKSDB_FUNC, "fixed key/value length is too large");
  fixed_key_len_ = int(fixed_key_len);
  fixed_value_len_ = fixed_value_len == uint64_t(-1) ? -1 : int(fixed_value_len);

  // Builder never emits SimpleTop with indexed_num==0 (empty → FinishAsEmptyTable).
  if (indexed_num_ == 0)
    throw Status::Corruption(ROCKSDB_FUNC, "zero records in non-empty SimpleTopTable");
  if (indexed_num_ > UINT64_MAX / 8 || table_properties_->tag_size != 8 * indexed_num_)
    throw Status::Corruption(ROCKSDB_FUNC, "invalid tag size");
  if (offset_bits_ > sizeof(size_t) * 8 || keylen_bits_ > sizeof(size_t) * 8)
    throw Status::Corruption(ROCKSDB_FUNC, "invalid index bit width");
  if (record_pool_size_ > file_data_.size_)
    throw Status::Corruption(ROCKSDB_FUNC, "record pool is outside the SST mmap");

  live_iter_num_ = 0;

  // Mode: offset_bits==0 => dual-fixed; else keylen_bits==0 => single-varlen.
  // fixed_value_len_ >= 0: fixed (0 = empty); < 0: variable.
  if (offset_bits_ == 0) {
    if (sstmeta_->index_bytes != 0 || keylen_bits_ != 0)
      throw Status::Corruption(ROCKSDB_FUNC, "invalid fixed-layout index");
    if (fixed_key_len_ <= 0 || fixed_value_len_ < 0)
      throw Status::Corruption(ROCKSDB_FUNC, "invalid fixed-layout key/value length");
    if (record_pool_size_ % indexed_num_ != 0)
      throw Status::Corruption(ROCKSDB_FUNC, "fixed-layout record pool is not divisible");
    record_stride_ = record_pool_size_ / indexed_num_;
    if (record_stride_ < size_t(fixed_key_len_) || record_stride_ - size_t(fixed_key_len_) > INT_MAX)
      throw Status::Corruption(ROCKSDB_FUNC, "invalid fixed-layout record stride");
    fixed_value_len_ = int(record_stride_ - size_t(fixed_key_len_));
  } else {
    if (sstmeta_->index_bytes == 0)
      throw Status::Corruption(ROCKSDB_FUNC, "missing variable-layout index");
    if (record_pool_size_ > SIZE_MAX - 63)
      throw Status::Corruption(ROCKSDB_FUNC, "record pool size overflow");
    size_t index_base_off = align_up(record_pool_size_, 64);
    if (index_base_off > file_data_.size_ || sstmeta_->index_bytes > file_data_.size_ - index_base_off)
      throw Status::Corruption(ROCKSDB_FUNC, "index is outside the SST mmap");
    auto* index_base = (unsigned char*)file_data_.data_ + index_base_off;
    index_bits_.risk_mmap_from(index_base, sstmeta_->index_bytes);
    size_t logical_bits;
    if (keylen_bits_ == 0) {
      // single-varlen: (key fixed & value var) or (key var & value fixed)
      if (!((fixed_key_len_ > 0 && fixed_value_len_ < 0) || (fixed_key_len_ == 0 && fixed_value_len_ >= 0)))
        throw Status::Corruption(ROCKSDB_FUNC, "invalid single-variable layout");
      if (indexed_num_ == SIZE_MAX || indexed_num_ + 1 > SIZE_MAX / offset_bits_)
        throw Status::Corruption(ROCKSDB_FUNC, "single-variable index size overflow");
      logical_bits = (indexed_num_ + 1) * offset_bits_;
    } else {
      if (fixed_key_len_ != 0 || fixed_value_len_ >= 0)
        throw Status::Corruption(ROCKSDB_FUNC, "invalid dual-variable layout");
      const size_t stride = offset_bits_ + keylen_bits_;
      if (indexed_num_ > (SIZE_MAX - offset_bits_) / stride)
        throw Status::Corruption(ROCKSDB_FUNC, "dual-variable index size overflow");
      logical_bits = indexed_num_ * stride + offset_bits_;
    }
    if (logical_bits > index_bits_.size())
      throw Status::Corruption(ROCKSDB_FUNC, "index is shorter than its logical size");
    size_t guard;
    if (keylen_bits_ == 0) {
      guard = index_bits_.get_uint<size_t>(indexed_num_ * offset_bits_,
                                           offset_bits_);
    } else {
      size_t stride = offset_bits_ + keylen_bits_;
      guard = index_bits_.get_uint<size_t>(indexed_num_ * stride, offset_bits_);
    }
    if (guard != record_pool_size_)
      throw Status::Corruption(ROCKSDB_FUNC, "invalid record-pool guard offset");
  }

  // TableReaderOptions::largest_seqno is only a candidate. A nonzero on-disk seqno means this is not an external all-seq-zero table.
  if (sstmeta_->max_seqno != 0)
    global_seqno_ = 0;
  ApplyGlobalSeqnoToRangeDel(file, tro, file_size, kSimpleTopTableMagic);
}

SimpleTopTableReader::~SimpleTopTableReader() {
  TERARK_VERIFY_F(0 == live_iter_num_, "real: %zd", live_iter_num_);
  index_bits_.risk_release_ownership();
}

uint64_t SimpleTopTableReader::ApproximateOffsetOf(
    ROCKSDB_8_X_COMMA(const ReadOptions&) const Slice& ikey,
    TableReaderCaller) {
  TERARK_VERIFY_GE(ikey.size(), 8u);
  Slice ukey(ikey.data(), ikey.size() - 8);
  size_t lo = LowerBoundUkey(ukey);
  return file_data_.size_ * lo / indexed_num_;
}

uint64_t SimpleTopTableReader::ApproximateSize(
    ROCKSDB_8_X_COMMA(const ReadOptions&) const Slice& start,
    const Slice& end, TableReaderCaller) {
  TERARK_VERIFY_GE(start.size(), 8u);
  TERARK_VERIFY_GE(end.size(), 8u);
  size_t lo = LowerBoundUkey(Slice(start.data(), start.size() - 8));
  size_t hi = LowerBoundUkey(Slice(end.data(), end.size() - 8));
  if (hi < lo) std::swap(lo, hi);
  return file_data_.size_ * (hi - lo) / indexed_num_;
}

template<bool kFixedKey, bool kFixedValue, bool kWithGlobalSeqno>
Status SimpleTopTableReader::GetTpl(const ReadOptions& ro, const Slice& key,
                                    GetContext* get_context) {
  ROCKSDB_ASSERT_GE(key.size(), 8);
  ParsedInternalKey target(key);
  auto [lo, hi] = EqualRangeUkeyLayout<kFixedKey, kFixedValue>(target.user_key);
  // pinning_tls/StartPin ⇒ internal_is_in_pinning_section: zero-copy pin
  // (noop Cleanable). Else nullptr ⇒ PinSelf copy.
  Cleanable noop_pinner, *pinner =
      ro.internal_is_in_pinning_section ? &noop_pinner : nullptr;
  for (size_t i = lo; i < hi; i++) {
    Slice ikey, val;
    RecAtTmpl<kFixedKey, kFixedValue>(i, &ikey, &val);
    ParsedInternalKey pikey;
    if constexpr (kWithGlobalSeqno) {
      pikey.user_key = Slice(ikey.data(), ikey.size() - 8);
      pikey.sequence = global_seqno_;
      pikey.type = ValueType(static_cast<unsigned char>(ikey.data()[ikey.size() - 8]));
    } else {
      pikey.FastParseInternalKey(ikey);
    }
    if (pikey.sequence > target.sequence) {
      continue;
    }
    if (ro.just_check_key_exists) {
      if (kTypeMerge == pikey.type) {
        pikey.type = kTypeValue;
      }
      val = Slice();
    }
    if (!get_context->SaveValue(pikey, val, pinner)) {
      break;
    }
  }
  return Status::OK();
}

Status SimpleTopTableReader::Get(const ReadOptions& ro, const Slice& key,
                                 GetContext* get_context,
                                 const SliceTransform*, bool) {
  auto dispatch = 4 * (fixed_key_len_ > 0)
                + 2 * (fixed_value_len_ >= 0)
                + 1 * NeedGlobalSeqnoRewrite(global_seqno_)
                ;
  switch (dispatch) {
  case 0b000: return GetTpl<0,0,0>(ro, key, get_context);
  case 0b001: return GetTpl<0,0,1>(ro, key, get_context);
  case 0b010: return GetTpl<0,1,0>(ro, key, get_context);
  case 0b011: return GetTpl<0,1,1>(ro, key, get_context);
  case 0b100: return GetTpl<1,0,0>(ro, key, get_context);
  case 0b101: return GetTpl<1,0,1>(ro, key, get_context);
  case 0b110: return GetTpl<1,1,0>(ro, key, get_context);
  case 0b111: return GetTpl<1,1,1>(ro, key, get_context);
  default: TERARK_DIE("invalid dispatch = %u", dispatch);
  }
}

std::string SimpleTopTableReader::ToWebViewString(const json&) const {
  char buf[256];
  snprintf(buf, sizeof(buf),
           "SimpleTopTableReader: n=%zu pool=%zu fixed_key=%d fixed_val=%d "
           "offset_bits=%zu keylen_bits=%zu",
           indexed_num_, record_pool_size_, fixed_key_len_, fixed_value_len_,
           offset_bits_, keylen_bits_);
  return buf;
}

/////////////////////////////////////////////////////////////////////////////
// Iterator — layout (kFixedKey×kFixedValue) + kWithGlobalSeqno (unlikely)
/////////////////////////////////////////////////////////////////////////////

template<bool Enabled>
struct SimpleTopIterSeqno {
  SequenceNumber global_seqno = 0;
  char* key_buf = nullptr;
  Slice Rewrite(Slice ikey) {
    memcpy(key_buf, ikey.data(), ikey.size());
    auto type = ValueType(static_cast<unsigned char>(ikey.data()[ikey.size() - 8]));
    EncodeFixed64(key_buf + ikey.size() - 8,
                  PackSequenceAndType(global_seqno, type));
    return Slice(key_buf, ikey.size());
  }
};
template<>
struct SimpleTopIterSeqno<false> {};

template<bool kFixedKey, bool kFixedValue, bool kWithGlobalSeqno>
class SimpleTopTableReader::Iter : public InternalIterator, boost::noncopyable {
public:
  const SimpleTopTableReader* tab_;
  size_t num_;
  intptr_t idx_;
  Slice key_;   // mmap pin; or seq_.key_buf when rewriting zero-seq
  Slice value_; // cached in LoadKey — avoid second RecAt on value()
  [[no_unique_address]] SimpleTopIterSeqno<kWithGlobalSeqno> seq_;

  explicit Iter(const SimpleTopTableReader* table, bool is_arena) {
    tab_ = table;
    num_ = table->indexed_num_;
    idx_ = -1;
    key_ = Slice();
    value_ = Slice();
    if constexpr (kWithGlobalSeqno) {
      seq_.global_seqno = table->global_seqno_;
      if (is_arena)
        seq_.key_buf = (char*)(this + 1);
      else
        seq_.key_buf = (char*)malloc(table->sstmeta_->max_ukey_len + 8);
    }
  }
  ~Iter() override {
    as_atomic(tab_->live_iter_num_).fetch_sub(1, std::memory_order_relaxed);
    if constexpr (kWithGlobalSeqno) {
      if (seq_.key_buf != (char*)(this + 1)) free(seq_.key_buf);
    }
  }

  void RecAt(size_t i, Slice* ikey, Slice* val) const {
    tab_->RecAtTmpl<kFixedKey, kFixedValue>(i, ikey, val);
  }

  void LoadKey() {
    ROCKSDB_ASSERT_LT(size_t(idx_), num_);
    Slice ikey, val;
    RecAt(size_t(idx_), &ikey, &val);
    value_ = val;
    if constexpr (!kWithGlobalSeqno) {
      key_ = ikey; // InternalKey as-is — pin mmap
    } else {
      key_ = seq_.Rewrite(ikey);
    }
  }

  void SetPinnedItersMgr(PinnedIteratorsManager*) final {}
  bool Valid() const final { return uint(idx_) < uint(num_); }
  void SeekForPrevAux(const Slice& target, const InternalKeyComparator& c) {
    SeekForPrevImpl(target, &c);
  }
  void SeekForPrev(const Slice& target) final {
    // concrete comparator — no virtual InternalKeyComparator::Compare
    if (tab_->isReverseBytewiseOrder_)
      SeekForPrevAux(target, InternalKeyComparator(ReverseBytewiseComparator()));
    else
      SeekForPrevAux(target, InternalKeyComparator(BytewiseComparator()));
  }
  Slice key() const final {
    TERARK_ASSERT_BT(idx_, 0, intptr_t(num_));
    return key_;
  }
  Slice user_key() const final {
    TERARK_ASSERT_BT(idx_, 0, intptr_t(num_));
    return Slice(key_.data(), key_.size() - 8);
  }
  Slice value() const final {
    TERARK_ASSERT_BT(idx_, 0, intptr_t(num_));
    return value_;
  }
  Status status() const final { return Status::OK(); }
  bool IsKeyPinned() const final {
    // with global_seqno, key may be rewritten into seq_.key_buf — never claim pinned
    return !kWithGlobalSeqno;
  }
  bool IsValuePinned() const final { return true; }

  void SeekToFirst() final {
    idx_ = 0;
    if (num_) LoadKey();
  }
  void SeekToLast() final {
    idx_ = intptr_t(num_) - 1;
    if (num_) LoadKey();
  }
  template<class UkeyCmp, class IkeyCmp>
  void SeekTmpl(const Slice& target, UkeyCmp ucmp, IkeyCmp icmp) {
    Slice ukey(target.data_, target.size_ - 8);
    size_t lo = tab_->LowerBoundUkeyLayout<kFixedKey, kFixedValue>(ukey);
    while (lo < num_) {
      Slice ikey, val;
      RecAt(lo, &ikey, &val);
      Slice cur_ukey(ikey.data(), ikey.size() - 8);
      if (ucmp(cur_ukey, ukey) != 0) break;
      // icmp(ikey, target) <=> ikey < target; stop when ikey >= target
      if constexpr (kWithGlobalSeqno) {
        auto type = ValueType(static_cast<unsigned char>(ikey.data()[ikey.size() - 8]));
        if (!icmp(ParsedInternalKey(cur_ukey, seq_.global_seqno, type), target)) break;
      } else {
        if (!icmp(ikey, target)) break;
      }
      lo++;
    }
    idx_ = intptr_t(lo);
    if (lo < num_) LoadKey();
  }
  void Seek(const Slice& target) final {
    ROCKSDB_VERIFY_GE(target.size_, 8);
    if (tab_->isReverseBytewiseOrder_)
      SeekTmpl(target, ReverseBytewiseCompareUserKeyNoTS(),
               RevBytewiseCompareInternalKey());
    else
      SeekTmpl(target, ForwardBytewiseCompareUserKeyNoTS(),
               BytewiseCompareInternalKey());
  }
  void Next() final {
    TERARK_ASSERT_BT(idx_, 0, intptr_t(num_));
    idx_++;
    if (uint(idx_) < uint(num_)) LoadKey();
  }
  void Prev() final {
    TERARK_ASSERT_BT(idx_, 0, intptr_t(num_));
    idx_--;
    if (idx_ >= 0) LoadKey();
  }
  bool NextAndGetResult(IterateResult* result) final {
    TERARK_ASSERT_BT(idx_, 0, intptr_t(num_));
    Next();
    if (Valid()) {
      result->SetKey(this->key());
      result->bound_check_result = IterBoundCheck::kUnknown;
      result->value_prepared = true;
      result->is_valid = true;
      return true;
    }
    result->is_valid = false;
    return false;
  }
};

template<bool kFixedKey, bool kFixedValue>
InternalIterator* SimpleTopTableReader::NewIterLayout(Arena* a) {
  if (UNLIKELY(NeedGlobalSeqnoRewrite(global_seqno_))) {
    using I = Iter<kFixedKey, kFixedValue, true>;
    size_t mem = sizeof(I) + sstmeta_->max_ukey_len + 8;
    if (a)
      return new (a->AllocateAligned(mem)) I(this, true);
    else
      return new I(this, false);
  } else {
    using I = Iter<kFixedKey, kFixedValue, false>;
    if (a)
      return new (a->AllocateAligned(sizeof(I))) I(this, true);
    else
      return new I(this, false);
  }
}

InternalIterator* SimpleTopTableReader::NewIterator(
    const ReadOptions&, const SliceTransform*, Arena* a, bool,
    TableReaderCaller, size_t, bool) {
  as_atomic(live_iter_num_).fetch_add(1, std::memory_order_relaxed);
  const bool fk = fixed_key_len_ > 0;
  const bool fv = fixed_value_len_ >= 0;
  if (fk && fv) return NewIterLayout<true, true>(a);
  if (fk) return NewIterLayout<true, false>(a);
  if (fv) return NewIterLayout<false, true>(a);
  return NewIterLayout<false, false>(a);
}

/////////////////////////////////////////////////////////////////////////////
// Factory
/////////////////////////////////////////////////////////////////////////////

TableBuilder* SimpleTopTableFactory::NewTableBuilder(
    const TableBuilderOptions& tbo, WritableFileWriter* file) const {
  TERARK_VERIFY(nullptr != tbo.ioptions.user_comparator);
  TERARK_VERIFY_F(IsBytewiseComparator(tbo.ioptions.user_comparator), "%s",
                  tbo.ioptions.user_comparator->Name());
  TERARK_VERIFY_EZ(tbo.ioptions.user_comparator->timestamp_size());
  TERARK_VERIFY_EZ(tbo.internal_comparator.user_comparator()->timestamp_size());
  if (0 == as_atomic(num_writers).fetch_add(1, std::memory_order_relaxed)) {
    start_time_point = g_pf.now();
  }
  return new SimpleTopTableBuilder(this, tbo, file);
}

Status SimpleTopTableFactory::NewTableReader(
    const ReadOptions&, const TableReaderOptions& tro,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table,
    bool prefetch_index_and_filter_in_cache) const try {
  (void)prefetch_index_and_filter_in_cache;
  auto t0 = g_pf.now();
  Slice file_data;
  file->exchange(new MmapReadWrapper(file));
  Status s = TopMmapReadAll(*file, file_size, &file_data);
  if (!s.ok()) {
    return s;
  }
  auto t1 = g_pf.now();
  MmapAdvSeq(file_data);
  auto t2 = g_pf.now();
  ROCKS_LOG_DEBUG(tro.ioptions.info_log,
      "NewTableReader(%s): mmap %.3f ms, warmup(madv_seq) %.3f ms",
      file->file_name().c_str(), g_pf.mf(t0, t1), g_pf.mf(t1, t2));
  auto t = new SimpleTopTableReader(this);
  table->reset(t);
  t->Open(file.release(), file_data, tro);
  as_atomic(num_readers).fetch_add(1, std::memory_order_relaxed);
  return Status::OK();
}
catch (const IOStatus& s) {
  WARN(tro.ioptions.info_log, "%s: Status: %s", ROCKSDB_FUNC, s.ToString().c_str());
  return Status::IOError(ROCKSDB_FUNC, s.ToString());
}
catch (const Status& s) {
  WARN(tro.ioptions.info_log, "%s: Status: %s", ROCKSDB_FUNC, s.ToString().c_str());
  return s;
}
catch (const std::exception& ex) {
  WARN(tro.ioptions.info_log, "%s: std::exception: %s", ROCKSDB_FUNC, ex.what());
  return Status::Corruption(ROCKSDB_FUNC, ex.what());
}
catch (TopEmptyTableReader* t) { // NOLINT
  TERARK_VERIFY(nullptr != t);
  table->reset(t);
  return Status::OK();
}

void SimpleTopTableFactory::Update(const json&, const json& js,
                                   const SidePluginRepo&) {
  ROCKSDB_JSON_OPT_ENUM(js, writeMethod);
  ROCKSDB_JSON_OPT_SIZE(js, fileWriteBufferSize);
  ROCKSDB_JSON_OPT_PROP(js, collectProperties);
  ROCKSDB_JSON_OPT_PROP(js, forceNeedCompact);
  ROCKSDB_JSON_OPT_PROP(js, debugLevel);
}

std::string SimpleTopTableFactory::GetPrintableOptions() const {
  SidePluginRepo* repo = nullptr;
  return ToString({}, *repo);
}

Status SimpleTopTableFactory::ValidateOptions(
    const DBOptions&, const ColumnFamilyOptions& cf_opts) const {
  if (!IsBytewiseComparator(cf_opts.comparator)) {
    return Status::InvalidArgument(ROCKSDB_FUNC, "comparator is not bytewise");
  }
  TERARK_VERIFY_EZ(cf_opts.comparator->timestamp_size());
  return Status::OK();
}

std::string SimpleTopTableFactory::ToString(const json& dump_options,
                                            const SidePluginRepo&) const {
  json djs;
  bool html = JsonSmartBool(dump_options, "html");
  ROCKSDB_JSON_SET_ENUM(djs, writeMethod);
  ROCKSDB_JSON_SET_SIZE(djs, fileWriteBufferSize);
  ROCKSDB_JSON_SET_PROP(djs, collectProperties);
  ROCKSDB_JSON_SET_PROP(djs, forceNeedCompact);
  ROCKSDB_JSON_SET_PROP(djs, debugLevel);
  ROCKSDB_JSON_SET_PROP(djs, start_time_point);
  ROCKSDB_JSON_SET_PROP(djs, build_time_duration);
  ROCKSDB_JSON_SET_PROP(djs, num_writers);
  ROCKSDB_JSON_SET_PROP(djs, num_readers);
  ROCKSDB_JSON_SET_PROP(djs, sum_user_key_cnt);
  ROCKSDB_JSON_SET_PROP(djs, sum_user_key_len);
  ROCKSDB_JSON_SET_SIZE(djs, sum_value_len);
  ROCKSDB_JSON_SET_SIZE(djs, sum_index_len);
  JS_TopTable_AddVersion(djs, html);
  JS_ToplingDB_AddVersion(djs, html);
  return JsonToString(djs, dump_options);
}

ROCKSDB_REG_Plugin("SimpleTopTable", SimpleTopTableFactory, TableFactory);
ROCKSDB_REG_EasyProxyManip("SimpleTopTable", SimpleTopTableFactory, TableFactory);
ROCKSDB_RegTableFactoryMagicNumber(kSimpleTopTableMagic, "SimpleTopTable");

} // namespace rocksdb
