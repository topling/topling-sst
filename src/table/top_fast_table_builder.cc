//
// Created by leipeng on 2020/8/23.
//
#include "top_fast_table.h"

#include "top_table_common.h"
#include "top_fast_table_internal.h"

#include <logging/logging.h>
#include <monitoring/iostats_context_imp.h>
#include <table/table_builder.h>
#include <table/block_based/block_builder.h>
#include <table/format.h>
#include <table/meta_blocks.h>
#include <rocksdb/merge_operator.h>

// terark headers
#if defined(_MSC_VER)
  #pragma warning(disable: 4245) // convert int to size_t in fsa of cspp
#endif
#include <terark/fsa/cspptrie.inl>
#include <terark/io/FileStream.hpp>
#include <terark/io/MemMapStream.hpp>
#include <terark/io/StreamBuffer.hpp>

#include "top_table_builder.h"

namespace ROCKSDB_NAMESPACE {

using namespace terark;

class TopFastTableBuilder : public TopTableBuilderBase {
public:
  TopFastTableBuilder(
        const TopFastTableFactory* table_factory,
        const TableBuilderOptions& tbo,
        WritableFileWriter* file);

  ~TopFastTableBuilder() override;
  void Add(const Slice& key, const Slice& value) final;
  Status Finish() final;
  void Abandon() final;
  uint64_t FileSize() const final;
  uint64_t EstimatedFileSize() const final;
  void FinishPrevUserKey();
  void FinishSamePrefix();
  void DoWriteAppend(const void* data, size_t size);
  void WriteValue(uint64_t seqvt, const Slice&);
  void ClearIndex();
  void ToplingFlushBuffer();

  const TopFastTableFactory* table_factory_;
  TableMultiPartInfo offset_info_;
  struct ValueNode {
    uint64_t pos;
    uint64_t seqvt;
  };
  valvec<ValueNode> valueNodeVec_; // of same user key
  union { MainPatricia cspp_; };
  union { MainPatricia::SingleWriterToken wtoken_; };

  fstring currentPrefix_;
  valvec<byte_t> prevUserKey_;
  OsFileStream fstream_;
  OutputBuffer fobuf_;
  MemMapStream fmap_;
  mutable size_t cspp_memsize_ = 0;
  uint32_t multi_num_ = 0;
  bool is_cspp_created_ = false;
  WriteMethod writeMethod_;
  long long t0 = 0;
};

TopFastTableBuilder::TopFastTableBuilder(
        const TopFastTableFactory* table_factory,
        const TableBuilderOptions& tbo,
        WritableFileWriter* file)
  : TopTableBuilderBase(tbo, file), table_factory_(table_factory)
{
  //size_t key_prefixLen = table_factory->table_options_.keyPrefixLen;
  uint32_t key_prefixLen = 0; // TODO: bugfix for non-zero key_prefixLen

  currentPrefix_.n = -1; // not equal to any string
  properties_.compression_name = "TooFast";

  writeMethod_ = table_factory->table_options_.writeMethod;
  // file_checksum_gen_factory must be null for non kRocksdbNative
  if (ioptions_.file_checksum_gen_factory) {
    if (WriteMethod::kRocksdbNative != writeMethod_) {
      WARN(ioptions_.info_log, "file_checksum_gen_factory is not null, use kRocksdbNative");
      writeMethod_ = WriteMethod::kRocksdbNative;
    }
  }
  if (WriteMethod::kToplingMmapWrite == writeMethod_) {
    // file_checksum_gen_factory must be null for non kRocksdbNative
    TERARK_VERIFY(nullptr == ioptions_.file_checksum_gen_factory);
    auto fd = file->writable_file()->FileDescriptor();
    auto& fname = file->file_name();
    try {
      // mmap does not support PROT_WRITE only
#if !defined(OS_WIN) && 0 // fcntl can not change open mode
      // Linux x86 does not allow O_WRONLY mmap PROT_WRITE only, and rocksdb
      // open PosixWritableFile as O_WRONLY, this will cause mmap fail.
      // We set file flags as O_RDWR to overcome this issue.
      if (fcntl(int(fd), F_SETFD, O_RDWR/* | O_CREAT | O_TRUNC*/) < 0) {
          assert(false); // abort on debug build
          writeMethod_ = WriteMethod::kRocksdbNative;
          ROCKS_LOG_WARN(ioptions_.info_log,
              "fcntl(%s, F_SETFD, O_RDWR) = %s, set as kRocksdbNative",
              fname.c_str(), strerror(errno));
      } else
#endif
      fmap_.dopen(fd, tbo.target_file_size, fname, O_RDWR);
    }
    catch (const std::exception& ex) {
      writeMethod_ = WriteMethod::kRocksdbNative;
      ROCKS_LOG_WARN(ioptions_.info_log,
          "fmap_.dopen(size=%lld, %s) = %s, set as kRocksdbNative",
          (llong)tbo.target_file_size, fname.c_str(), ex.what());
    }
  }
  else if (WriteMethod::kToplingFileWrite == writeMethod_) {
    // file_checksum_gen_factory must be null for non kRocksdbNative
    TERARK_VERIFY(nullptr == ioptions_.file_checksum_gen_factory);
    int fd = (int)file_->writable_file()->FileDescriptor();
    TERARK_VERIFY_GE(fd, 0);
    fstream_.attach(fd);
    fobuf_.attach(&fstream_);
    fobuf_.initbuf(table_factory->table_options_.fileWriteBufferSize);
  }

  valueNodeVec_.reserve(64);
  offset_info_.offset_.reserve(64);
  offset_info_.prefixSet_.m_fixlen = key_prefixLen;
  offset_info_.prefixSet_.m_strpool.reserve(256);
}

TopFastTableBuilder::~TopFastTableBuilder() {
  ClearIndex();
}

uint64_t TopFastTableBuilder::FileSize() const {
  if (terark_likely(is_cspp_created_)) {
    maximize(cspp_memsize_, cspp_.mem_size_inline());
    return offset_ + cspp_memsize_;
  }
  else {
    return offset_;
  }
}

uint64_t TopFastTableBuilder::EstimatedFileSize() const {
  if (terark_likely(is_cspp_created_)) {
    maximize(cspp_memsize_, cspp_.mem_size_inline());
    // + properties_.num_entries is to make it monotonically increasing
    return offset_ + cspp_memsize_ + properties_.num_entries;
  }
  else {
    return offset_;
  }
}

void TopFastTableBuilder::Add(const Slice& key, const Slice& value) try {
  properties_.num_entries++;
  properties_.raw_key_size += key.size();
  properties_.raw_value_size += value.size();
  const uint64_t seqvt = DecodeFixed64(key.data() + key.size() - 8);
  const auto vt = ValueType(seqvt & 255u);
  TERARK_ASSERT_EZ((vt & 0x80u));
  if (IsValueType(vt)) {
    if (vt == kTypeDeletion || vt == kTypeSingleDeletion) {
      properties_.num_deletions++;
    } else if (vt == kTypeMerge) {
      properties_.num_merge_operands++;
    }
    TERARK_ASSERT_GE(key.size(), 8);
    const size_t pref_len = offset_info_.prefixSet_.m_fixlen;
    const fstring userKey(key.data(), key.size() - 8);
    TERARK_ASSERT_GE(userKey.size(), pref_len);
    if (LIKELY(currentPrefix_ == userKey.substr(0, pref_len))) {
      // same prefix
      //userKey = userKey.substr(pref_len);
      if (prevUserKey_ != userKey) {
        assert((prevUserKey_ < userKey) ^ isReverseBytewiseOrder_);
        FinishPrevUserKey();
        //ks_.minKeyLen = std::min(userKey.size(), ks_.minKeyLen);
        //ks_.maxKeyLen = std::max(userKey.size(), ks_.maxKeyLen);
        prevUserKey_.assign(userKey);
      }
    }
    else { // prefix changed
      if (UNLIKELY(-1 == currentPrefix_.n)) {
        t0 = g_pf.now();
      }
      else {
        FinishSamePrefix();
      }
      // now start a new prefix
      size_t IndexEntrySize = sizeof(TopFastIndexEntry);
      // NOLINTNEXTLINE
      ROCKSDB_VERIFY(!is_cspp_created_);
      new(&cspp_)MainPatricia(IndexEntrySize, 512u<<10u, Patricia::SingleThreadStrict);
      new(&wtoken_)MainPatricia::WriterToken();
      wtoken_.acquire(&cspp_);
      is_cspp_created_ = true;
      cspp_memsize_ = 0;

      // for next (Index,Store) pair
      offset_info_.offset_.push_back({size_t(offset_), 0, 0, 0});
      offset_info_.prefixSet_.push_back(fstring(key.data(), pref_len));
      currentPrefix_ = offset_info_.prefixSet_.back();
      prevUserKey_.assign(userKey);
    }
    WriteValue(seqvt, value);
  }
  else if (vt == kTypeRangeDeletion) {
    range_del_block_.Add(key, value);
    properties_.num_range_deletions++;
  }
  else {
    const char* ename = enum_name(vt).data();
    // use full ename such as: "kTypeLogData = 0x3"
    TERARK_DIE("unexpected ValueType = %s(%d)", ename, vt);
  }
  uint64_t estimate_offset = EstimatedFileSize();
  NotifyCollectTableCollectorsOnAdd(key, value, estimate_offset,
                                    collectors_, ioptions_.info_log.get());
}
catch (const IOStatus& s) {
  TERARK_VERIFY(!s.ok());
  WARN_EXCEPT(ioptions_.info_log
      , "%s: IOStatus: %s", BOOST_CURRENT_FUNCTION, s.ToString().c_str());
  io_status_ = s;
}
catch (const Status& s) {
  TERARK_VERIFY(!s.ok());
  WARN_EXCEPT(ioptions_.info_log
      , "%s: Status: %s", BOOST_CURRENT_FUNCTION, s.ToString().c_str());
  status_ = s;
}
catch (const std::exception& ex) {
  WARN_EXCEPT(ioptions_.info_log
      , "%s: std::exception: %s", BOOST_CURRENT_FUNCTION, ex.what());
  status_ = Status::Corruption(ROCKSDB_FUNC, ex.what());
}

void TopFastTableBuilder::FinishPrevUserKey() {
  TopFastIndexEntry entry; // NOLINT
  entry.seqvt = valueNodeVec_[0].seqvt;
  const size_t  valueNum = valueNodeVec_.size();
  if (valueNum >= 2) {
    size_t seqvtSize = sizeof(uint64_t) * (valueNum - 1);
    size_t posSize = sizeof(uint32_t) * (valueNum + 1);
    size_t allocPos = cspp_.mem_alloc(seqvtSize + posSize);
    TERARK_ASSERT_NE(allocPos, MainPatricia::mem_alloc_fail); // never fail
    auto posArr = (uint32_t*)cspp_.mem_get(allocPos);
    auto seqArr = (uint64_t*)(posArr + valueNum + 1);
    for (size_t i = 0; i < valueNum; ++i) {
      posArr[i] = uint32_t(valueNodeVec_[i].pos);
    }
    posArr[valueNum] = uint32_t(offset_);
    for (size_t i = 0; i < valueNum-1; ++i) {
      // seqArr is aligned to 4, not 8, use unaligned_save
      unaligned_save(seqArr, i, valueNodeVec_[i+1].seqvt);
    }
    entry.valuePos = (uint32_t)allocPos;
    entry.valueLen = (uint32_t)valueNum;
    entry.valueMul = 1;
    multi_num_++;
  }
  else {
    assert(valueNodeVec_.size() == 1);
    uint64_t pos = valueNodeVec_[0].pos;
    entry.valuePos = uint32_t(pos);
    entry.valueLen = uint32_t(offset_ - pos);
    entry.valueMul = 0;
  }
  TERARK_VERIFY(cspp_.insert(prevUserKey_, &entry, &wtoken_));
  valueNodeVec_.erase_all();
  num_user_key_++;
}

void TopFastTableBuilder::FinishSamePrefix() {
  FinishPrevUserKey();

  // now write index

  using namespace std::placeholders;
  // NOLINTNEXTLINE
  auto writeAppend = std::bind(&TopFastTableBuilder::DoWriteAppend, this, _1, _2);

  Padzero<64>(writeAppend, offset_);

  auto& meta = offset_info_.offset_.back();
  meta.value = offset_ - meta.offset;

  size_t indexOffset = offset_;
  cspp_.save_mmap(writeAppend);
  TERARK_VERIFY_AL(offset_, 64);
  meta.key = offset_ - indexOffset; // index size
  ClearIndex();
}

void TopFastTableBuilder::ClearIndex() {
  if (is_cspp_created_) {
    wtoken_.release();
    wtoken_.~SingleWriterToken();
    cspp_.~MainPatricia();
    is_cspp_created_ = false;
  }
}

void TopFastTableBuilder::DoWriteAppend(const void* data, size_t size) {
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

// now just called by Add(key, value)
terark_forceinline
void TopFastTableBuilder::WriteValue(uint64_t seqvt, const Slice& value) {
  valueNodeVec_.push_back({offset_, seqvt});
  DoWriteAppend(value.data_, value.size_);
}

const uint64_t kTopFastTableMagic = 0x747361466b726154; // TarkFast
// const is default internal linkage, use extern to force external linkage,
// will also be used in top_fast_table_reader.cc
extern const std::string kTopFastTableOffsetBlock;
const std::string kTopFastTableOffsetBlock = "TopFastTableOffsetBlock";

Status TopFastTableBuilder::Finish() try {
  if (0 == num_user_key_ && valueNodeVec_.empty()) {
    ToplingFlushBuffer();
    FinishAsEmptyTable();
    return Status::OK();
  }
  TERARK_VERIFY(!valueNodeVec_.empty());
  FinishSamePrefix();
  TERARK_VERIFY_EQ(offset_info_.offset_.size(), offset_info_.prefixSet_.size());
  size_t index_size = 0;
  size_t value_size = 0;
  for (auto& x : offset_info_.offset_) {
    index_size += x.key;
    value_size += x.value;
  }
  offset_info_.offset_.push_back({size_t(offset_), 0, 0, 0});
  const size_t fixlen = offset_info_.prefixSet_.m_fixlen;
  TERARK_VERIFY_EQ(offset_info_.prefixSet_.str_size(),
                   offset_info_.prefixSet_.size() * fixlen);
  if (fixlen && isReverseBytewiseOrder_) {
    // reverse prefixSet_ to forward bytewise order
    auto dst = (byte_t*)malloc(offset_info_.prefixSet_.str_size());
    if (nullptr == dst) { // should not happen
      return Status::MemoryLimit(ROCKSDB_FUNC, "prefixSet_.str_size()");
    }
    auto origin_dst = dst;
    byte_t* src = offset_info_.prefixSet_.m_strpool.end();
    for (size_t i = offset_info_.prefixSet_.size(); i > 0; i--) {
      src -= fixlen;
      memcpy(dst, src, fixlen);
      dst += fixlen;
    }
    ::free(src); // now src is point to malloc address
    offset_info_.prefixSet_.m_strpool.risk_set_data(origin_dst);
  }
  properties_.index_size = index_size;
  properties_.data_size  = value_size;
  properties_.num_data_blocks = offset_info_.prefixSet_.size();
  ToplingFlushBuffer();
  auto writeAppend = [this](const void* buf, size_t len) {
    IOStatus s = file_->Append(Slice((const char*)buf, len));
    if (!s.ok()) throw s;
    offset_ += len;
  };
  Padzero<16>(writeAppend, offset_);
  WriteMeta(kTopFastTableMagic,
    {{kTopFastTableOffsetBlock,
      WriteBlock(offset_info_.dump(), file_, &offset_)}});

  auto fac = table_factory_;
  auto ukey_len = properties_.raw_key_size - 8 * properties_.num_entries;
  long long t1 = g_pf.now();
  auto td = g_pf.us(t1 - t0);
  as_atomic(fac->sum_user_key_cnt_).fetch_add(num_user_key_, std::memory_order_relaxed);
  as_atomic(fac->sum_user_key_len_).fetch_add(ukey_len, std::memory_order_relaxed);
  as_atomic(fac->sum_full_key_len_).fetch_add(properties_.raw_key_size, std::memory_order_relaxed);
  as_atomic(fac->sum_value_len_).fetch_add(properties_.raw_value_size, std::memory_order_relaxed);
  as_atomic(fac->sum_entry_cnt_).fetch_add(properties_.num_entries, std::memory_order_relaxed);
  as_atomic(fac->sum_index_num_).fetch_add(offset_info_.prefixSet_.size(), std::memory_order_relaxed);
  as_atomic(fac->sum_index_len_).fetch_add(index_size, std::memory_order_relaxed);
  as_atomic(fac->sum_multi_num_).fetch_add(multi_num_, std::memory_order_relaxed);
  as_atomic(fac->build_time_duration_).fetch_add(td, std::memory_order_relaxed); // in us

  closed_ = true;
  return Status::OK();
}
catch (const IOStatus& s) {
  ClearIndex();
  io_status_ = s;
  closed_ = true;
  return Status::IOError(ROCKSDB_FUNC, s.ToString());
}
catch (const Status& s) {
  ClearIndex();
  status_ = s;
  closed_ = true;
  return s;
}
catch (const std::exception& ex) {
  ClearIndex();
  status_ = Status::Corruption(ROCKSDB_FUNC, ex.what());
  closed_ = true;
  return status_;
}

void TopFastTableBuilder::Abandon() {
  fobuf_.resetbuf(); // discard buffer content
  ToplingFlushBuffer();
  ClearIndex();
  closed_ = true;
}

void TopFastTableBuilder::ToplingFlushBuffer() {
  if (WriteMethod::kToplingMmapWrite == writeMethod_) {
    TERARK_VERIFY_EQ(fmap_.tell(), offset_);
    fmap_.close();
    auto fd = (int)file_->writable_file()->FileDescriptor();
    lseek(fd, offset_, SEEK_SET);
    file_->SetFileSize(offset_); // fool the WritableFileWriter
    file_->writable_file()->SetFileSize(offset_); // fool the WritableFile
    IOSTATS_ADD(bytes_written, offset_);
  }
  else if (WriteMethod::kToplingFileWrite == writeMethod_) {
    fobuf_.flush_buffer();
    fstream_.detach();
    file_->SetFileSize(offset_); // fool the WritableFileWriter
    file_->writable_file()->SetFileSize(offset_); // fool the WritableFile
    IOSTATS_ADD(bytes_written, offset_);
  }
  else {
    TERARK_VERIFY(WriteMethod::kRocksdbNative == writeMethod_);
    file_->Flush();
  }
}

// be KISS, do not play petty trick
//size_t GetFixedPrefixLen(const SliceTransform* tr);

TableBuilder*
TopFastTableFactory::NewTableBuilder(const TableBuilderOptions& tbo,
                                        WritableFileWriter* file)
const {
  if (!table_options_.useFilePreallocation)
    file->writable_file()->SetPreallocationBlockSize(0);

  TERARK_VERIFY(nullptr != tbo.ioptions.user_comparator);
  TERARK_VERIFY(nullptr != tbo.ioptions.user_comparator->Name());
  TERARK_VERIFY_F(IsBytewiseComparator(tbo.ioptions.user_comparator), "%s",
                  tbo.ioptions.user_comparator->Name());
  TERARK_VERIFY_EZ(tbo.ioptions.user_comparator->timestamp_size());
  TERARK_VERIFY_EZ(tbo.internal_comparator.user_comparator()->timestamp_size());
  TERARK_VERIFY_F(strcmp(tbo.internal_comparator.user_comparator()->Name(),
                         tbo.ioptions.user_comparator->Name()) == 0,
                  "%s <=> %s",
                  tbo.internal_comparator.user_comparator()->Name(),
                  tbo.ioptions.user_comparator->Name());
  if (0 == as_atomic(num_writers_).fetch_add(1, std::memory_order_relaxed)) {
    start_time_point_ = g_pf.now();
    WARN(tbo.ioptions.info_log,
        "ToplingFastTable is deprecated, use SingleFastTable instead");
  }
  return new TopFastTableBuilder(this, tbo, file);
}

} // namespace ROCKSDB_NAMESPACE

