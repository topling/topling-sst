//
// Created by leipeng on 2021-11-14
//
// This file is copied from top_fast_table_builder.cc with simplify
// multi-part to single-part
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
#include <terark/fsa/cspptrie.inl>
#include <terark/io/FileStream.hpp>
#include <terark/io/MemMapStream.hpp>
#include <terark/io/StreamBuffer.hpp>

#include "top_table_builder.h"

namespace ROCKSDB_NAMESPACE {

using namespace terark;

class SingleFastTableBuilder : public TopTableBuilderBase {
public:
  SingleFastTableBuilder(
        const TopFastTableFactory* table_factory,
        const TableBuilderOptions& tbo,
        WritableFileWriter* file);

  ~SingleFastTableBuilder() override;
  void Add(const Slice& key, const Slice& value) final;
  Status Finish() final;
  void Abandon() final;
  uint64_t EstimatedFileSize() const final;

  void FinishPrevUserKey();
  void DoWriteAppend(const void* data, size_t size);
  void WriteValue(uint64_t seqvt, const Slice&);
  void ToplingFlushBuffer();

  const TopFastTableFactory* table_factory_;
  struct ValueNode {
    uint64_t pos;
    uint64_t seqvt;
  };
  valvec<ValueNode> valueNodeVec_; // of same user key
  size_t max_key_len = 0, min_key_len = SIZE_MAX;
  size_t max_val_len = 0, min_val_len = SIZE_MAX;
  MainPatricia cspp_;
  MainPatricia::SingleWriterToken wtoken_;
  valvec<byte_t> prevUserKey_;
  OsFileStream fstream_;
  OutputBuffer fobuf_;
  MemMapStream fmap_;
  mutable size_t cspp_memsize_ = 0;
  uint32_t multi_num_ = 0;
  bool is_wtoken_released_ = false;
  WriteMethod writeMethod_;
  long long t0 = 0;
};

constexpr size_t IndexEntrySize = sizeof(TopFastIndexEntry);
const std::string kCSPPIndex = "CSPPIndex";

SingleFastTableBuilder::SingleFastTableBuilder(
        const TopFastTableFactory* table_factory,
        const TableBuilderOptions& tbo,
        WritableFileWriter* file)
  : TopTableBuilderBase(tbo, file), table_factory_(table_factory)
  , cspp_(IndexEntrySize, 2u<<20u, Patricia::SingleThreadStrict)
{
  properties_.compression_name = "SngFast";
  writeMethod_ = table_factory->table_options_.writeMethod;
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
  wtoken_.acquire(&cspp_);
  t0 = g_pf.now();
}

SingleFastTableBuilder::~SingleFastTableBuilder() {
  if (!is_wtoken_released_) {
    wtoken_.release();
  }
}

uint64_t SingleFastTableBuilder::EstimatedFileSize() const {
  maximize(cspp_memsize_, cspp_.mem_size_inline());
  // + properties_.num_entries is to make it monotonically increasing
  return offset_ + cspp_memsize_ + properties_.num_entries;
}

void SingleFastTableBuilder::Add(const Slice& key, const Slice& value) try {
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
    const fstring userKey(key.data(), key.size() - 8);
    if (LIKELY(num_user_key_ > 0)) {
      if (prevUserKey_ != userKey) {
        assert((prevUserKey_ < userKey) ^ isReverseBytewiseOrder_);
        FinishPrevUserKey();
        prevUserKey_.assign(userKey);
        num_user_key_++;
      }
    } else {
      prevUserKey_.assign(userKey);
      num_user_key_++;
    }
    maximize(max_key_len, userKey.size());
    minimize(min_key_len, userKey.size());
    maximize(max_val_len, value.size());
    minimize(min_val_len, value.size());
    WriteValue(seqvt, value);
  }
  else if (vt == kTypeRangeDeletion) {
    range_del_block_.Add(key, value);
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

void SingleFastTableBuilder::FinishPrevUserKey() {
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
  TERARK_VERIFY(wtoken_.value() != nullptr);
  valueNodeVec_.erase_all();
}

void SingleFastTableBuilder::DoWriteAppend(const void* data, size_t size) {
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
void SingleFastTableBuilder::WriteValue(uint64_t seqvt, const Slice& value) {
  valueNodeVec_.push_back({offset_, seqvt});
  DoWriteAppend(value.data_, value.size_);
}

const uint64_t kSingleFastTableMagic = 0x747361466c676e53; // SnglFast

Status SingleFastTableBuilder::Finish() try {
  if (0 == num_user_key_) {
    FinishAsEmptyTable();
    return Status::OK();
  }
  TERARK_VERIFY(!valueNodeVec_.empty());
  FinishPrevUserKey();

  //---- Write CSPP Index
  wtoken_.release();
  is_wtoken_released_ = true;
  using namespace std::placeholders;
  // NOLINTNEXTLINE
  auto writeAppend = std::bind(&SingleFastTableBuilder::DoWriteAppend, this, _1, _2);
  Padzero<64>(writeAppend, offset_);
  size_t indexOffset = offset_;
  cspp_.set_readonly();
  cspp_.save_mmap(writeAppend);
  //---- End Write CSPP Index
  size_t index_size = offset_ - indexOffset;
  properties_.index_size = index_size;
  properties_.data_size  = indexOffset;
  properties_.num_data_blocks = 1;
  if (max_key_len == min_key_len) {
    properties_.fixed_key_len = max_key_len;
  }
  if (max_val_len == min_val_len) {
    properties_.fixed_value_len = max_val_len;
  }

  ToplingFlushBuffer();
  WriteMeta(kSingleFastTableMagic, {{kCSPPIndex, {indexOffset, index_size}}});

  auto fac = table_factory_;
  auto ukey_len = properties_.raw_key_size - 8 * properties_.num_entries;
  long long t1 = g_pf.now();
  auto td = g_pf.us(t1 - t0);
  as_atomic(fac->sum_user_key_cnt_).fetch_add(num_user_key_, std::memory_order_relaxed);
  as_atomic(fac->sum_user_key_len_).fetch_add(ukey_len, std::memory_order_relaxed);
  as_atomic(fac->sum_full_key_len_).fetch_add(properties_.raw_key_size, std::memory_order_relaxed);
  as_atomic(fac->sum_value_len_).fetch_add(properties_.raw_value_size, std::memory_order_relaxed);
  as_atomic(fac->sum_entry_cnt_).fetch_add(properties_.num_entries, std::memory_order_relaxed);
  as_atomic(fac->sum_index_num_).fetch_add(1, std::memory_order_relaxed);
  as_atomic(fac->sum_index_len_).fetch_add(index_size, std::memory_order_relaxed);
  as_atomic(fac->sum_multi_num_).fetch_add(multi_num_, std::memory_order_relaxed);
  as_atomic(fac->build_time_duration_).fetch_add(td, std::memory_order_relaxed); // in us

  closed_ = true;
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

void SingleFastTableBuilder::Abandon() {
  wtoken_.release();
  is_wtoken_released_ = true;
  fobuf_.resetbuf(); // discard buffer content
  ToplingFlushBuffer();
  closed_ = true;
}

void SingleFastTableBuilder::ToplingFlushBuffer() {
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
SingleFastTableFactory::NewTableBuilder(const TableBuilderOptions& tbo,
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
  }
  return new SingleFastTableBuilder(this, tbo, file);
}

} // namespace ROCKSDB_NAMESPACE

