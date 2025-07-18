//
// Created by leipeng on 2021-11-14
//
// This file is copied from top_fast_table_reader.cc with simplify
// multi-part to single-part
//

#include "top_fast_table.h"
#include "top_table_reader.h"
#include "top_table_common.h"
#include "top_fast_table_internal.h"

#include <logging/logging.h>
#include <table/table_reader.h>
#include <table/meta_blocks.h>
#include <util/thread_local.h>
#if defined(_MSC_VER)
  #pragma warning(disable: 4245) // convert int to size_t in fsa of cspp
  #pragma warning(disable: 4673)
#endif
#include <terark/fsa/cspptrie.inl>
#include <terark/num_to_str.hpp>
#include <terark/thread/fiber_aio.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <topling/side_plugin_factory.h>
#include <topling/builtin_table_factory.h>

namespace ROCKSDB_NAMESPACE {

using namespace terark;

#if defined(_MSC_VER)
// off_t on msvc is uint32
intptr_t pread(intptr_t fd, void* buf, size_t len, size_t offset) {
  OVERLAPPED ol{};
  ol.Offset = DWORD(offset);
  ol.OffsetHigh = DWORD(offset >> 32);
  DWORD res = 0;
  if (ReadFile(HANDLE(fd), buf, DWORD(len), &res, &ol)) {
    return intptr_t(res);
  } else {
    return -1;
  }
}
#define fiber_aio_read pread
#endif

static void PatriciaIterUnref(void* obj) {
  auto iter = (Patricia::Iterator*)(obj);
  iter->dispose();
}
class SingleFastTableReader : public TopTableReaderBase {
public:
  ~SingleFastTableReader() override;
  explicit SingleFastTableReader() : iter_cache_(&PatriciaIterUnref) {}
  void Open(RandomAccessFileReader*, Slice file_data, const TableReaderOptions&, WarmupLevel);
  InternalIterator*
  NewIterator(const ReadOptions&, const SliceTransform* prefix_extractor,
              Arena* arena, bool skip_filters, TableReaderCaller caller,
              size_t compaction_readahead_size,
              bool allow_unprepared_value) final;

  uint64_t ApproximateOffsetOf(ROCKSDB_8_X_COMMA(const ReadOptions& readopt) const Slice& key, TableReaderCaller) final;
  uint64_t ApproximateSize(ROCKSDB_8_X_COMMA(const ReadOptions& readopt) const Slice&, const Slice&, TableReaderCaller) final;
  void SetupForCompaction() final;
  void Prepare(const Slice& /*target*/) final;
  size_t ApproximateMemoryUsage() const final;

  bool ReadValue(Slice* val, size_t len, size_t offset,
                 const ReadOptions&, Cleanable* pinner, Status*);
  Status Get(const ReadOptions& readOptions, const Slice& key,
                     GetContext* get_context,
                     const SliceTransform* prefix_extractor,
                     bool skip_filters) final;

  Status VerifyChecksum(const ReadOptions&, TableReaderCaller) final;
  bool GetRandomInternalKeysAppend(size_t num, std::vector<std::string>* output) const final;
#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 70060
  Status ApproximateKeyAnchors(const ReadOptions&, std::vector<Anchor>&) override;
#endif
  std::string FirstInternalKey(Slice user_key, MainPatricia::SingleReaderToken&) const;

  std::string ToWebViewString(const json& dump_options) const final;
  bool IsMyFactory(const TableFactory* fac) const final {
    return fac == factory_;
  }

  Patricia::Iterator* TLS_Iter() {
    auto iter = (Patricia::Iterator*)iter_cache_.Get();
    if (UNLIKELY(!iter)) {
      iter = cspp_.new_iter();
      iter_cache_.Reset(iter);
    }
    return iter;
  }
  size_t FirstValuePos(const Patricia::TokenBase& token) const {
    auto entry = cspp_.value_of<TopFastIndexEntry>(token);
    return entry.valueMul
           ? aligned_load<uint32_t>(cspp_.mem_get(entry.valuePos))
           : entry.valuePos;
  }
  double SizeCoefficient() const {
    double sum_val_len = std::max<double>(index_offset_, 1.0);
    return file_data_.size_ / sum_val_len;
  }

// data member also public
  int level_ = -1; // from ReadOptions::level
  int fd_ = -1;
  unsigned avg_val_len_ = 0;
  mutable MainPatricia cspp_;
  union { ThreadLocalPtr iter_cache_; }; // for ApproximateOffsetOf
  size_t index_offset_; // also sum_value_len
  size_t index_size_;
  const SingleFastTableFactory* factory_ = nullptr;

  class Iter;
  class BaseIter;
  class RevIter;
};

uint64_t SingleFastTableReader::ApproximateOffsetOf(
      ROCKSDB_8_X_COMMA(const ReadOptions& readopt)
      const Slice& ikey, TableReaderCaller) {
  auto iter = TLS_Iter();
  fstring user_key(ikey.data(), ikey.size() - 8);
  if (iter->seek_lower_bound(user_key)) {
    return FirstValuePos(*iter) * SizeCoefficient();
  }
  return file_data_.size_;
}
uint64_t SingleFastTableReader::ApproximateSize(ROCKSDB_8_X_COMMA(const ReadOptions& readopt)
                                                const Slice& beg, const Slice& end,
                                                TableReaderCaller) {
  auto iter = TLS_Iter();
  fstring ukeyBeg(beg.data_, beg.size_ - 8);
  fstring ukeyEnd(end.data_, end.size_ - 8);
  double coefficient = SizeCoefficient();
  size_t vposBeg = index_offset_, vposEnd = index_offset_;
  if (iter->seek_lower_bound(ukeyBeg)) {
    vposBeg = FirstValuePos(*iter);
  }
  if (iter->seek_lower_bound(ukeyEnd)) {
    vposEnd = FirstValuePos(*iter);
  }
  if (vposBeg < vposEnd)
    return uint64_t(coefficient * (vposEnd - vposBeg));
  else
    return uint64_t(coefficient * (vposBeg - vposEnd));
}

void SingleFastTableReader::SetupForCompaction() {
  MmapAdvSeq(file_data_);
}

void SingleFastTableReader::Prepare(const Slice& /*target*/) {
}

size_t SingleFastTableReader::ApproximateMemoryUsage() const {
  return index_size_;
}

bool SingleFastTableReader::ReadValue(Slice* val, size_t len, size_t offset,
                                      const ReadOptions& read_options,
                                      Cleanable* pinner, Status* st) {
  assert(!pinner->HasCleanups());
  if (UNLIKELY(offset + len > index_offset_)) {
    string_appender<> msg;
    msg | "SST Get: read(" | file_->file_name()
        | ", len " | len | ", offset " | offset | ")"
        | " offset + len > index_offset_ " | index_offset_
        | " should be data corruption";
    *st = Status::Corruption(msg);
    return false;
  }
  bool use_pread =
      avg_val_len_ >= unsigned(factory_->table_options_.minPreadLen) &&
      avg_val_len_ <= unsigned(factory_->table_options_.maxPreadLen) &&
      level_ >= factory_->table_options_.minPreadLevel;
  if (0 == len || (read_options.internal_is_in_pinning_section && !use_pread)) {
    // use mmap with zero copy
    if (UNLIKELY(len >= size_t(factory_->table_options_.maxPreadLen))) {
      MmapWarmUpBytes(file_data_.data_ + offset, len);
    }
    val->data_ = file_data_.data_ + offset;
    val->size_ = len;
    return true;
  }
  auto buf = (char*)malloc(len);
  if (UNLIKELY(!buf)) {
    string_appender<> msg;
    msg | __FILE__ | ":" | __LINE__ | ": ReadValue in ";
    msg | "SST Get: read(" | file_->file_name()
        | ", len " | len | ", offset " | offset | ") malloc fail";
    *st = Status::MemoryLimit(msg, strerror(errno));
    return false;
  }
  if (use_pread) {
    auto [func_ptr, func_name] = read_options.async_io
         ? std::make_pair(fiber_aio_read, "fiber_aio_read")
         : std::make_pair(         pread,          "pread");
    auto rlen = func_ptr(fd_, buf, len, offset);
    if (size_t(rlen) != len) {
      string_appender<> msg;
      msg | func_name | "(" | file_->file_name()
          | ", len " | len | ", offset " | offset | ") = " | rlen;
      *st = Status::IOError(msg, strerror(errno));
      free(buf);
      return false;
    }
  } else { // use mmap with memcpy
    if (UNLIKELY(len >= size_t(factory_->table_options_.maxPreadLen))) {
      MmapWarmUpBytes(file_data_.data_ + offset, len);
    }
    memcpy(buf, file_data_.data_ + offset, len);
  }
  // trick: extra param arg2 passed to free will be ignored
  TOPLING_GCC_NOLINT(-Wcast-function-type);
  auto clean = (Cleanable::CleanupFunction)(free);
  pinner->RegisterCleanup(clean, buf, nullptr);
  val->data_ = buf;
  val->size_ = len;
  return true;
}

Status SingleFastTableReader::Get(const ReadOptions& readOptions,
                                  const Slice& ikey,
                                  GetContext* get_context,
                                  const SliceTransform* prefix_extractor,
                                  bool skip_filters) {
  ROCKSDB_ASSERT_GE(ikey.size(), kNumInternalBytes);
  ParsedInternalKey pikey(ikey);
  Status st;
  MainPatricia::SingleReaderToken token(&cspp_);
  if (!cspp_.lookup(pikey.user_key, &token)) {
    return st;
  }
  bool const just_check_key_exists = readOptions.just_check_key_exists;
  auto entry = cspp_.value_of<TopFastIndexEntry>(token);
  const SequenceNumber finding_seq = pikey.sequence;
  Slice val;
  Cleanable pinner[1]; // empty pinner for zero copy, [1] for min code change
  auto do_read = [&](size_t len, size_t offset) {
    return ReadValue(&val, len, offset, readOptions, pinner, &st);
  };
  if (entry.valueMul) {
    size_t valueNum = entry.valueLen;
    TERARK_ASSERT_GE(valueNum, 2);
    auto posArr = (const uint32_t*)cspp_.mem_get(entry.valuePos);
    auto seqArr = (const uint64_t*)(posArr + valueNum + 1);
    UnPackSequenceAndType(entry.seqvt, &pikey.sequence, &pikey.type);
    if (pikey.sequence <= finding_seq) {
      if (!just_check_key_exists) {
        if (!do_read(posArr[1] - posArr[0], posArr[0])) {
          return st;
        }
      }
      else {
        if (kTypeMerge == pikey.type) {
          pikey.type = kTypeValue; // instruct SaveValue to stop earlier
        }
      }
      if (!get_context->SaveValue(pikey, val, pinner)) {
        return st;
      }
    }
    size_t lo = lower_bound_seq(seqArr, valueNum-1, finding_seq);
    for (; lo < valueNum-1; lo++) {
      if (!just_check_key_exists) {
        size_t val_beg = posArr[lo+1];
        size_t val_end = posArr[lo+2];
        if (!do_read(val_end - val_beg, val_beg)) {
          return st;
        }
      }
      else {
        if (kTypeMerge == pikey.type) {
          pikey.type = kTypeValue; // instruct SaveValue to stop earlier
        }
        val = Slice();
      }
      auto seqvt = unaligned_load<uint64_t>(seqArr, lo);
      UnPackSequenceAndType(seqvt, &pikey.sequence, &pikey.type);
      TERARK_ASSERT_LE(pikey.sequence, finding_seq);
      if (!get_context->SaveValue(pikey, val, pinner)) {
        return st;
      }
    }
  }
  else {
    UnPackSequenceAndType(entry.seqvt, &pikey.sequence, &pikey.type);
    if (pikey.sequence == 0) {
      pikey.sequence = global_seqno_;
    }
    if (pikey.sequence <= finding_seq) {
      if (!just_check_key_exists) {
        if (!do_read(entry.valueLen, entry.valuePos)) {
          return st;
        }
      }
      else {
        if (kTypeMerge == pikey.type) {
          pikey.type = kTypeValue; // instruct SaveValue to stop earlier
        }
      }
      get_context->SaveValue(pikey, val, pinner);
    }
  }
  return st;
}

Status SingleFastTableReader::VerifyChecksum(const ReadOptions&, TableReaderCaller) {
  return Status::OK();
}

// for gdb debug purpose, defined in top_fast_table.cc
size_t DumpInternalIterator(InternalIterator* iter, size_t num, bool hex);

class SingleFastTableReader::BaseIter : public InternalIterator, boost::noncopyable {
public:
  const SingleFastTableReader* tab_;
  const char* file_mem_;
  Patricia::Iterator* iter_;
  typedef bool (*DfaIterScanFN)(ADFA_LexIterator*);
 #if defined(_MSC_VER) || defined(__clang__)
 #else
  #pragma GCC diagnostic ignored "-Wpmf-conversions"
  DfaIterScanFN dfa_iter_next_;
  DfaIterScanFN dfa_iter_prev_;
 #endif
  uint64_t global_seqno_;
  int val_idx_;
  int val_num_;
  uint32_t val_pos_;
  uint32_t val_len_;
  const uint64_t* seq_arr_;
  const uint32_t* pos_arr_;

  explicit BaseIter(const SingleFastTableReader* table) { // NOLINT
    file_mem_ = table->file_data_.data();
    tab_ = table;
    iter_ = table->cspp_.new_iter();
   #if defined(_MSC_VER) || defined(__clang__)
   #else
    dfa_iter_next_ = (DfaIterScanFN)(iter_->*(&ADFA_LexIterator::incr));
    dfa_iter_prev_ = (DfaIterScanFN)(iter_->*(&ADFA_LexIterator::decr));
   #endif
    global_seqno_ = table->global_seqno_;
    SetInvalid();
  }
 #if defined(_MSC_VER) || defined(__clang__)
  bool InvokeDfaIterNext() { return iter_->incr(); }
  bool InvokeDfaIterPrev() { return iter_->decr(); }
 #else
  bool InvokeDfaIterNext() { return dfa_iter_next_(iter_); }
  bool InvokeDfaIterPrev() { return dfa_iter_prev_(iter_); }
 #endif
  ~BaseIter() override {
    iter_->dispose();
    as_atomic(tab_->live_iter_num_).fetch_sub(1, std::memory_order_relaxed);
  }
  void SetInvalid() {
    val_idx_ = -1;
    val_num_ = 0;
    val_pos_ = UINT32_MAX;
    val_len_ = UINT32_MAX;
    seq_arr_ = nullptr;
    pos_arr_ = nullptr;
  }
  void SetPinnedItersMgr(PinnedIteratorsManager*) final {}
  bool Valid() const final { return -1 != val_idx_; }
  void SeekForPrevAux(const Slice& target, const InternalKeyComparator& c) {
    SeekForPrevImpl(target, &c);
  }
  void SeekForPrev(const Slice& target) final {
    if (tab_->isReverseBytewiseOrder_)
      SeekForPrevAux(target, InternalKeyComparator(ReverseBytewiseComparator()));
    else
      SeekForPrevAux(target, InternalKeyComparator(BytewiseComparator()));
  }
  void SetAtFirstValue() {
    auto entry = tab_->cspp_.value_of<TopFastIndexEntry>(*iter_);
    if (entry.valueMul) {
      val_num_ = entry.valueLen;
      assert(val_num_ >= 2);
      pos_arr_ = (const uint32_t*)tab_->cspp_.mem_get(entry.valuePos);
      seq_arr_ = (const uint64_t*)(pos_arr_ + val_num_ + 1);
      val_pos_ = pos_arr_[0];
      val_len_ = pos_arr_[1] - pos_arr_[0];
    } else {
      val_num_ = 1;
      pos_arr_ = nullptr;
      seq_arr_ = nullptr;
      val_pos_ = entry.valuePos;
      val_len_ = entry.valueLen;
      if (entry.seqvt >> 8 == 0)
        entry.seqvt = PackSequenceAndType(global_seqno_, ValueType(entry.seqvt));
    }
    val_idx_ = 0;
    unaligned_save(iter_->mutable_word().ensure_unused(8), entry.seqvt);
  }
  void SetAtLastValue() {
    auto entry = tab_->cspp_.value_of<TopFastIndexEntry>(*iter_);
    if (entry.valueMul) {
      val_num_ = entry.valueLen;
      assert(val_num_ >= 2);
      pos_arr_ = (const uint32_t*)tab_->cspp_.mem_get(entry.valuePos);
      seq_arr_ = (const uint64_t*)(pos_arr_ + val_num_ + 1);
      val_idx_ = val_num_ - 1;
      val_pos_ = pos_arr_[val_idx_];
      val_len_ = pos_arr_[val_idx_ + 1] - val_pos_;
      entry.seqvt = unaligned_load<uint64_t>(seq_arr_, val_num_ - 2); // last
    } else {
      val_num_ = 1;
      pos_arr_ = nullptr;
      seq_arr_ = nullptr;
      val_idx_ = 0;
      val_pos_ = entry.valuePos;
      val_len_ = entry.valueLen;
      if (entry.seqvt >> 8 == 0)
        entry.seqvt = PackSequenceAndType(global_seqno_, ValueType(entry.seqvt));
    }
    unaligned_save(iter_->mutable_word().ensure_unused(8), entry.seqvt);
  }
  void SeekSeq(uint64_t seq) {
    auto entry = tab_->cspp_.value_of<TopFastIndexEntry>(*iter_);
    if (entry.valueMul) {
      size_t vnum = entry.valueLen;
      val_num_ = entry.valueLen;
      assert(val_num_ >= 2);
      pos_arr_ = (const uint32_t*)tab_->cspp_.mem_get(entry.valuePos);
      seq_arr_ = (const uint64_t*)(pos_arr_ + vnum + 1);
      if ((entry.seqvt >> 8) <= seq) {
        unaligned_save(iter_->mutable_word().ensure_unused(8), entry.seqvt);
        val_pos_ = pos_arr_[0];
        val_len_ = pos_arr_[1] - pos_arr_[0];
        val_idx_ = 0;
      }
      else {
        size_t lo = lower_bound_seq(seq_arr_, vnum-1, seq);
        if (lo < vnum-1) {
          unaligned_save(iter_->mutable_word().ensure_unused(8),
                         unaligned_load<uint64_t>(seq_arr_, lo));
          val_idx_ = int(lo + 1);
          val_pos_ = pos_arr_[lo + 1];
          val_len_ = pos_arr_[lo + 2] - val_pos_;
        }
        else
          val_idx_ = int(lo), Next();
      }
    }
    else {
      val_idx_ = 0;
      val_num_ = 1;
      pos_arr_ = nullptr;
      seq_arr_ = nullptr;
      val_pos_ = entry.valuePos;
      val_len_ = entry.valueLen;
      if (entry.seqvt >> 8 == 0)
        entry.seqvt = PackSequenceAndType(global_seqno_, ValueType(entry.seqvt));
      if ((entry.seqvt >> 8) <= seq)
        unaligned_save(iter_->mutable_word().ensure_unused(8), entry.seqvt);
      else
        Next();
    }
  }
  Slice key() const final {
    TERARK_ASSERT_BT(val_idx_, 0, val_num_);
    fstring word = iter_->word();
    TERARK_ASSERT_GE(iter_->mutable_word().capacity(), word.size() + 8);
    return Slice(word.data(), word.size() + 8); // NOLINT
  }
  Slice user_key() const final {
    TERARK_ASSERT_BT(val_idx_, 0, val_num_);
    fstring word = iter_->word();
    TERARK_ASSERT_GE(iter_->mutable_word().capacity(), word.size() + 8);
    return Slice(word.data(), word.size()); // NOLINT
  }
  Slice value() const final {
    TERARK_ASSERT_BT(val_idx_, 0, val_num_);
    return Slice(file_mem_ + val_pos_, val_len_); // NOLINT
  }
  bool PrepareAndGetValue(Slice* v) final {
    TERARK_ASSERT_BT(val_idx_, 0, val_num_);
    v->data_ = file_mem_ + val_pos_;
    v->size_ = val_len_;
    return true;
  }
  Status status() const final { return Status::OK(); }
  bool IsKeyPinned() const final { return false; }
  bool IsValuePinned() const final { return true; }

  bool NextAndGetResult(IterateResult* result) noexcept final {
    if (LIKELY(NextAndCheckValid())) {
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
class SingleFastTableReader::Iter : public BaseIter {
public:
  using BaseIter::BaseIter;
  void SeekToFirst() final {
    TERARK_VERIFY(iter_->seek_begin());
    SetAtFirstValue();
  }
  void SeekToLast() final {
    TERARK_VERIFY(iter_->seek_end());
    SetAtLastValue();
  }
  void Seek(const Slice& target) final {
    ROCKSDB_ASSERT_GE(target.size(), kNumInternalBytes);
    ParsedInternalKey pikey(target);
    if (UNLIKELY(!iter_->seek_lower_bound(pikey.user_key))) {
      SetInvalid();
      return;
    }
    if (iter_->word() == pikey.user_key)
      SeekSeq(pikey.sequence);
    else
      SetAtFirstValue();
  }
  void Next() final {
    NextAndCheckValid(); // ignore return value
  }
  bool NextAndCheckValid() final {
    TERARK_ASSERT_BT(val_idx_, 0, val_num_);
    if (++val_idx_ < val_num_) {
      assert(val_num_ >= 2);
      auto seqvt = unaligned_load<uint64_t>(seq_arr_, val_idx_-1);
      unaligned_save(iter_->mutable_word().end(), seqvt);
      val_pos_ = pos_arr_[val_idx_];
      val_len_ = pos_arr_[val_idx_ + 1] - val_pos_;
      return true;
    }
    if (UNLIKELY(!InvokeDfaIterNext())) {
      SetInvalid();
      return false;
    }
    SetAtFirstValue();
    return true;
  }
  void Prev() final {
    PrevAndCheckValid(); // ignore return value
  }
  bool PrevAndCheckValid() final {
    TERARK_ASSERT_BT(val_idx_, 0, val_num_);
    if (--val_idx_ >= 0) {
      uint64_t seqvt = val_idx_ > 0
                     ? unaligned_load<uint64_t>(seq_arr_, val_idx_-1)
                     : tab_->cspp_.value_of<TopFastIndexEntry>(*iter_).seqvt;
      if (seqvt >> 8 == 0)
        seqvt = PackSequenceAndType(global_seqno_, ValueType(seqvt));
      unaligned_save(iter_->mutable_word().end(), seqvt);
      val_pos_ = pos_arr_[val_idx_];
      val_len_ = pos_arr_[val_idx_ + 1] - val_pos_;
      return true;
    }
    if (UNLIKELY(!InvokeDfaIterPrev())) {
      SetInvalid();
      return false;
    }
    SetAtLastValue();
    return true;
  }
};
class SingleFastTableReader::RevIter : public BaseIter {
public:
  using BaseIter::BaseIter;
  void SeekToFirst() final {
    TERARK_VERIFY(iter_->seek_end());
    SetAtFirstValue();
  }
  void SeekToLast() final {
    TERARK_VERIFY(iter_->seek_begin());
    SetAtLastValue();
  }
  void Seek(const Slice& target) final {
    ROCKSDB_ASSERT_GE(target.size(), kNumInternalBytes);
    ParsedInternalKey pikey(target);
    if (iter_->seek_lower_bound(pikey.user_key)) {
      if (iter_->word() != pikey.user_key && !InvokeDfaIterPrev()) {
        // reach ReverseBytewise end of trie
        SetInvalid();
        return;
      }
    }
    else { // ReverseBytewise begin of trie
      TERARK_VERIFY(iter_->seek_end());
    }
    // now iter is ReverseBytewise lower_bound
    if (iter_->word() == pikey.user_key)
      SeekSeq(pikey.sequence);
    else
      SetAtFirstValue();
  }
  void Next() final {
    NextAndCheckValid(); // ignore return value
  }
  bool NextAndCheckValid() final {
    TERARK_ASSERT_BT(val_idx_, 0, val_num_);
    if (++val_idx_ < val_num_) {
      assert(val_num_ >= 2);
      auto seqvt = unaligned_load<uint64_t>(seq_arr_, val_idx_-1);
      unaligned_save(iter_->mutable_word().end(), seqvt);
      val_pos_ = pos_arr_[val_idx_];
      val_len_ = pos_arr_[val_idx_ + 1] - val_pos_;
      return true;
    }
    if (UNLIKELY(!InvokeDfaIterPrev())) {
      SetInvalid();
      return false;
    }
    SetAtFirstValue();
    return true;
  }
  void Prev() final {
    PrevAndCheckValid(); // ignore return value
  }
  bool PrevAndCheckValid() final {
    TERARK_ASSERT_BT(val_idx_, 0, val_num_);
    if (--val_idx_ >= 0) {
      uint64_t seqvt = val_idx_ > 0
                     ? unaligned_load<uint64_t>(seq_arr_, val_idx_-1)
                     : tab_->cspp_.value_of<TopFastIndexEntry>(*iter_).seqvt;
      if (seqvt >> 8 == 0)
        seqvt = PackSequenceAndType(global_seqno_, ValueType(seqvt));
      unaligned_save(iter_->mutable_word().end(), seqvt);
      val_pos_ = pos_arr_[val_idx_];
      val_len_ = pos_arr_[val_idx_ + 1] - val_pos_;
      return true;
    }
    if (UNLIKELY(!InvokeDfaIterNext())) {
      SetInvalid();
      return false;
    }
    SetAtLastValue();
    return true;
  }
};
InternalIterator*
SingleFastTableReader::NewIterator(const ReadOptions& ro,
                                   const SliceTransform* prefix_extractor,
                                   Arena* a,
                                   bool skip_filters,
                                   TableReaderCaller caller,
                                   size_t compaction_readahead_size,
                                   bool allow_unprepared_value)
{
  as_atomic(live_iter_num_).fetch_add(1, std::memory_order_relaxed);
  using RevI = RevIter;
  if (isReverseBytewiseOrder_)
    return a ? new(a->AllocateAligned(sizeof(RevI)))RevI(this) : new RevI(this);
  else
    return a ? new(a->AllocateAligned(sizeof(Iter)))Iter(this) : new Iter(this);
}

bool SingleFastTableReader::GetRandomInternalKeysAppend(
                  size_t num, std::vector<std::string>* output) const {
  SortableStrVec keys;
  cspp_.dfa_get_random_keys(&keys, num);
  MainPatricia::SingleReaderToken token(&cspp_);
  for (size_t i = 0; i < keys.size(); ++i) {
    fstring onekey = keys[i];
    output->push_back(FirstInternalKey(SliceOf(onekey), token));
  }
  return true;
}

#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 70060
Status SingleFastTableReader::
ApproximateKeyAnchors(const ReadOptions& ro, std::vector<Anchor>& anchors) {
  if (!factory_->table_options_.accurateKeyAnchorsSize) {
    return TopTableReaderBase::ApproximateKeyAnchors(ro, anchors);
  }
  size_t an_num = 256;
  if (size_t keyAnchorSizeUnit = factory_->table_options_.keyAnchorSizeUnit) {
    size_t units = file_data_.size_ / keyAnchorSizeUnit;
    an_num = std::min<size_t>(units, 10000);
    an_num = std::max<size_t>(an_num, 256);
  }
  SortableStrVec keys;
  cspp_.dfa_get_random_keys(&keys, std::min(cspp_.num_words(), an_num));
  keys.sort();
  if (isReverseBytewiseOrder_) {
    std::reverse(keys.m_index.begin(), keys.m_index.end());
  }
  auto iter = TLS_Iter();
  anchors.reserve(keys.size() + 1);
  double coefficient = SizeCoefficient();
  size_t prev_offset = 0;
  for (size_t i = 0; i < keys.size(); ++i) {
    while (i + 1 < keys.size() && keys[i] == keys[i + 1]) {
      i++; // skip dup key
    }
    Slice user_key = SliceOf(keys[i]);
    TERARK_VERIFY(cspp_.lookup(user_key, iter));
    size_t val_pos = FirstValuePos(*iter);
    size_t curr_offset = val_pos * coefficient;
    ROCKSDB_VERIFY_GE(curr_offset, prev_offset);
    if (curr_offset > prev_offset) {
      anchors.push_back({user_key, curr_offset - prev_offset});
      prev_offset = curr_offset;
    }
    else if (curr_offset && curr_offset == prev_offset) {
      STD_WARN("ApproximateKeyAnchors: same offset = %zd", curr_offset);
    }
  }
  if (isReverseBytewiseOrder_) {
    ROCKSDB_VERIFY(iter->seek_begin());
  } else {
    ROCKSDB_VERIFY(iter->seek_end());
  }
  Slice largest_user_key = SliceOf(iter->word());
  if (UNLIKELY(keys.size() && SliceOf(keys.back()) != largest_user_key)) {
    size_t val_pos = FirstValuePos(*iter);
    size_t curr_offset = val_pos * coefficient;
    anchors.push_back({largest_user_key, curr_offset - prev_offset});
  }
  return Status::OK();
}
#endif

std::string SingleFastTableReader::FirstInternalKey(
        Slice user_key, MainPatricia::SingleReaderToken& token) const {
  TERARK_VERIFY(cspp_.lookup(user_key, &token));
  auto entry = cspp_.value_of<TopFastIndexEntry>(token);
  std::string ikey;
  ikey.reserve(user_key.size_ + 8);
  ikey.append(user_key.data_, user_key.size_);
  ikey.append((char*)&entry.seqvt, 8);
  return ikey;
}

std::string SingleFastTableReader::ToWebViewString(const json& dump_options) const {
  json djs;
  auto& props = *table_properties_;
  djs["Props.User"] = TableUserPropsToJson(props.user_collected_properties, dump_options);
  djs["num_entries"] = props.num_entries;
  djs["num_deletions"] = props.num_deletions;
  djs["num_merges"] = props.num_merge_operands;
  djs["num_range_del"] = props.num_range_deletions;
  djs["fixed_key_len"] = props.fixed_key_len;
  djs["fixed_value_len"] = (long long)props.fixed_value_len;
  djs["data"] = {
    {"size", props.data_size},
    {"avg", props.data_size / double(props.num_entries) },
  };
  djs["raw"] = {
    {"sum", {
      {"key_size", SizeToString(props.raw_key_size)},
      {"value_size", SizeToString(props.raw_value_size)},
    }},
    {"avg", {
      {"key_size", props.raw_key_size*1.0/props.num_entries},
      {"value_size", props.raw_value_size*1.0/props.num_entries},
    }},
  };
  djs["cspp"] = {
    {"keys", cspp_.num_words()},
    {"offset", index_offset_},
    {"size", SizeToString(cspp_.mem_size_inline())},
    {"avg_key", cspp_.mem_size_inline()*1.0/cspp_.num_words() - sizeof(TopFastIndexEntry)},
    {"IndexEntrySize", sizeof(TopFastIndexEntry)},
    {"zpath_states", cspp_.num_zpath_states()},
  };
  return JsonToString(djs, dump_options);
}

/////////////////////////////////////////////////////////////////////////////

void SingleFastTableReader::Open(RandomAccessFileReader* file, Slice file_data,
                                 const TableReaderOptions& tro,
                                 WarmupLevel warmupLevel) {
  uint64_t file_size = file_data.size_;
  try {
    LoadCommonPart(file, tro, file_data, kSingleFastTableMagic);
  }
  catch (const Status&) { // very rare, try EmptyTable
    BlockContents emptyTableBC = ReadMetaBlockE(
        file, file_size, kTopEmptyTableMagicNumber,
        tro.ioptions, kTopEmptyTableKey);
    TERARK_VERIFY(!emptyTableBC.data.empty());
    INFO(tro.ioptions.info_log,
         "SingleFastTableReader::Open: %s is EmptyTable, it's ok\n",
         file->file_name().c_str());
    auto t = UniquePtrOf(new TopEmptyTableReader());
    file_.release(); // NOLINT
    t->Open(file, file_data, tro);
    throw t.release(); // NOLINT
  }
  if (!fstring(table_properties_->compression_options).strstr("allseq0")) {
    // special case: if entry.valueMul, global_seqno_ must be 0
    global_seqno_ = 0;
  }
  auto props = table_properties_.get();
  if (0 == props->tag_size) {
    auto entries = props->num_entries;
    props->index_size -= entries * 8;
    props->tag_size = entries * 8;
  }
  BlockContents indexBlock = ReadMetaBlockE(file, file_size,
        kSingleFastTableMagic, tro.ioptions, kCSPPIndex);
  index_size_ = indexBlock.data.size_;
  index_offset_ = indexBlock.data.data_ - file_data_.data_;
  live_iter_num_ = 0;
  level_ = tro.level;
  avg_val_len_ = unsigned(index_offset_ / props->num_entries);

  TERARK_VERIFY_AL(index_offset_, 64);
  if (tro.ioptions.advise_random_on_open) {
    if (warmupLevel < WarmupLevel::kValue)
      MmapAdvRnd(file_data_.data_, index_offset_); // value mem
  }
  if (WarmupLevel::kIndex == warmupLevel) {
    MmapWarmUp(indexBlock.data);
  }
  cspp_.self_mmap_user_mem(indexBlock.data);
  if (!props->compression_options.empty()) {
    props->compression_options += ";";
  }
  props->compression_options += "Free = ";
  props->compression_options += SizeToString(cspp_.mem_frag_size());
  char buf[32];
  auto len = sprintf(buf, ", %.2f%%", 100.0*cspp_.mem_frag_size()/file_size);
  props->compression_options.append(buf, len);
}

SingleFastTableReader::~SingleFastTableReader() {
  TERARK_VERIFY_F(0 == live_iter_num_, "real: %zd", live_iter_num_);
  iter_cache_.~ThreadLocalPtr(); // destruct before cspp_
}

Status
SingleFastTableFactory::NewTableReader(
            const ReadOptions& ro,
            const TableReaderOptions& tro,
            std::unique_ptr<RandomAccessFileReader>&& file,
            uint64_t file_size,
            std::unique_ptr<TableReader>* table,
            bool prefetch_index_and_filter_in_cache)
const try {
  (void)prefetch_index_and_filter_in_cache; // now ignore
  auto t0 = g_pf.now();
  int fd = (int)file->target()->FileDescriptor();
  Slice file_data;
  file->exchange(new MmapReadWrapper(file));
  Status s = TopMmapReadAll(*file, file_size, &file_data);
  if (!s.ok()) {
    return s;
  }
  auto t1 = g_pf.now();
  if (WarmupLevel::kValue == table_options_.warmupLevel) {
    MmapAdvSeq(file_data);
    MmapWarmUp(file_data);
  }
  auto t2 = g_pf.now();
  ROCKS_LOG_DEBUG(tro.ioptions.info_log,
      "NewTableReader(%s): mmap %.3f ms, warmup(%s) %.3f ms",
      file->file_name().c_str(), g_pf.mf(t0,t1),
      enum_cstr(table_options_.warmupLevel), g_pf.mf(t1,t2));
  auto t = new SingleFastTableReader();
  table->reset(t);
  t->fd_ = fd;
  t->factory_ = this;
  t->Open(file.release(), file_data, tro, table_options_.warmupLevel);
  as_atomic(num_readers_).fetch_add(1, std::memory_order_relaxed);
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

} // ROCKSDB_NAMESPACE
