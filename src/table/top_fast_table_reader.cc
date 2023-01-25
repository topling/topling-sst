//
// Created by leipeng on 2020/8/23.
//

#include "top_fast_table.h"
#include "top_table_reader.h"
#include "top_table_common.h"
#include "top_fast_table_internal.h"

#include <logging/logging.h>
#include <table/table_reader.h>
#include <table/meta_blocks.h>
#include <util/thread_local.h>
#include <terark/fsa/cspptrie.inl>

#include <topling/side_plugin_factory.h>
#include <topling/builtin_table_factory.h>

namespace ROCKSDB_NAMESPACE {

using namespace terark;

static void PatriciaIterUnref(void* obj) {
  auto iter = (Patricia::Iterator*)(obj);
  iter->dispose();
}

class TopFastTableReader : public TopTableReaderBase {
public:
  ~TopFastTableReader() override;
  TopFastTableReader() : iter_cache_(&PatriciaIterUnref) {}
  void Open(RandomAccessFileReader*, Slice file_data, const TableReaderOptions&, WarmupLevel);
  InternalIterator*
  NewIterator(const ReadOptions&, const SliceTransform* prefix_extractor,
              Arena* arena, bool skip_filters, TableReaderCaller caller,
              size_t compaction_readahead_size,
              bool allow_unprepared_value) final;

  uint64_t ApproximateOffsetOf(const Slice& key, TableReaderCaller) final;
  uint64_t ApproximateSize(const Slice&, const Slice&, TableReaderCaller) final;
  void SetupForCompaction() final;
  void Prepare(const Slice& /*target*/) final;
  size_t ApproximateMemoryUsage() const final;
  Status Get(const ReadOptions& readOptions, const Slice& key,
                     GetContext* get_context,
                     const SliceTransform* prefix_extractor,
                     bool skip_filters) final;

  Status VerifyChecksum(const ReadOptions&, TableReaderCaller) final;
  bool GetRandomInteranlKeysAppend(size_t num, std::vector<std::string>* output) const final;
  std::string FirstInternalKey(Slice user_key, MainPatricia::SingleReaderToken&) const;

  std::string ToWebViewString(const json& dump_options) const final;

// data member also public
  valvec<std::unique_ptr<MainPatricia> > cspp_;
  ThreadLocalPtr iter_cache_; // for ApproximateOffsetOf
  TableMultiPartInfo offset_info_;
  size_t index_size_;

  class Iter;
  class BaseIter;
  class RevIter;
};

uint64_t TopFastTableReader::ApproximateOffsetOf(const Slice& ikey, TableReaderCaller) {
  // we ignore seqnum of ikey
  fstring user_key(ikey.data(), ikey.size() - 8);
  size_t lower;
  if (isReverseBytewiseOrder_) {
    lower = offset_info_.prefixSet_.upper_bound(user_key);
    lower = offset_info_.prefixSet_.size() - lower; // reverse
  }
  else {
    lower = offset_info_.prefixSet_.lower_bound_prefix(user_key);
  }
  size_t val_pos = file_data_.size_;
  if (lower < cspp_.size()) {
    size_t part_val = offset_info_.offset_[lower+0].value;
    size_t part_beg = offset_info_.offset_[lower+0].offset;
    size_t part_end = offset_info_.offset_[lower+1].offset;
    size_t part_len = part_end - part_beg;
    size_t pref_len = offset_info_.prefixSet_.m_fixlen;
    val_pos = part_beg;
    if (user_key.startsWith(offset_info_.prefixSet_[lower])) {
      MainPatricia* trie = cspp_[lower].get();
      auto iter = (Patricia::Iterator*)iter_cache_.Get();
      if (UNLIKELY(!iter)) {
        iter = trie->new_iter();
        iter_cache_.Reset(iter);
      }
      ROCKSDB_ASSERT_EQ(iter->get_dfa(), trie); // now cspp_.size() must be 1
      if (iter->seek_lower_bound(user_key.substr(pref_len))) {
        auto entry = iter->value_of<TopFastIndexEntry>();
        if (entry.valueMul)
          val_pos = aligned_load<uint32_t>(trie->mem_get(entry.valuePos));
        else
          val_pos = entry.valuePos;

        double coefficient = double(part_len) / (part_val + 1.0);
        size_t relative_pos = val_pos - part_beg;
        val_pos = part_beg + size_t(relative_pos * coefficient);
      }
    }
  }
  return val_pos;
}

uint64_t TopFastTableReader::ApproximateSize(const Slice& beg, const Slice& end,
                                                TableReaderCaller caller) {
  uint64_t offset_beg = ApproximateOffsetOf(beg, caller);
  uint64_t offset_end = ApproximateOffsetOf(end, caller);
  return offset_end - offset_beg;
}

void TopFastTableReader::SetupForCompaction() {
}

void TopFastTableReader::Prepare(const Slice& /*target*/) {
}

size_t TopFastTableReader::ApproximateMemoryUsage() const {
  return index_size_;
}

Status TopFastTableReader::Get(const ReadOptions& readOptions,
                                  const Slice& ikey,
                                  GetContext* get_context,
                                  const SliceTransform* prefix_extractor,
                                  bool skip_filters) {
  ROCKSDB_ASSERT_GE(ikey.size(), kNumInternalBytes);
  ParsedInternalKey pikey(ikey);
  Status st;
  MainPatricia* trie;
  if (size_t pref_len = offset_info_.prefixSet_.m_fixlen) {
    //TERARK_ASSERT_GE(pikey.user_key.size(), pref_len); // not must
    if (UNLIKELY(pikey.user_key.size() < pref_len)) {
      return st;
    }
    size_t lo;
    if (isReverseBytewiseOrder_) {
      lo = offset_info_.prefixSet_.upper_bound_fixed(pikey.user_key.data());
      if (UNLIKELY(0 == lo)) {
        return st;
      }
      lo = offset_info_.prefixSet_.size() - lo; // reverse
    }
    else {
      lo = offset_info_.prefixSet_.lower_bound_fixed(pikey.user_key.data());
      if (UNLIKELY(lo >= cspp_.size())) {
        return st;
      }
    }
    if (UNLIKELY(memcmp(offset_info_.prefixSet_[lo].data(),
                        pikey.user_key.data(), pref_len) != 0)) {
      return st;
    }
    trie = cspp_[lo].get();
  }
  else {
    trie = cspp_[0].get();
  }
  MainPatricia::SingleReaderToken token(trie);
  if (!trie->lookup(pikey.user_key, &token)) {
    return st;
  }
  bool const just_check_key_exists = readOptions.just_check_key_exists;
  auto entry = token.value_of<TopFastIndexEntry>();
  bool matched;
  const SequenceNumber finding_seq = pikey.sequence;
  Slice val;
  Cleanable noop_pinner;
  if (entry.valueMul) {
    size_t valueNum = entry.valueLen;
    TERARK_ASSERT_GE(valueNum, 2);
    auto posArr = (const uint32_t*)trie->mem_get(entry.valuePos);
    auto seqArr = (const uint64_t*)(posArr + valueNum + 1);
    UnPackSequenceAndType(entry.seqvt, &pikey.sequence, &pikey.type);
    if (pikey.sequence <= finding_seq) {
      if (!just_check_key_exists) {
        val.data_ = file_data_.data_ + posArr[0];
        val.size_ = posArr[1] - posArr[0];
      }
      else {
        if (kTypeMerge == pikey.type) {
          pikey.type = kTypeValue; // instruct SaveValue to stop earlier
        }
      }
      if (!get_context->SaveValue(pikey, val, &matched, &noop_pinner)) {
        return st;
      }
    }
    size_t lo = lower_bound_seq(seqArr, valueNum-1, finding_seq);
    for (; lo < valueNum-1; lo++) {
      if (!just_check_key_exists) {
        size_t val_beg = posArr[lo+1];
        size_t val_end = posArr[lo+2];
        val.data_ = file_data_.data_ + val_beg;
        val.size_ = val_end - val_beg;
      }
      else {
        if (kTypeMerge == pikey.type) {
          pikey.type = kTypeValue; // instruct SaveValue to stop earlier
        }
      }
      auto seqvt = unaligned_load<uint64_t>(seqArr, lo);
      UnPackSequenceAndType(seqvt, &pikey.sequence, &pikey.type);
      TERARK_ASSERT_LE(pikey.sequence, finding_seq);
      if (!get_context->SaveValue(pikey, val, &matched, &noop_pinner)) {
        return st;
      }
    }
  }
  else {
    UnPackSequenceAndType(entry.seqvt, &pikey.sequence, &pikey.type);
    if (pikey.sequence <= finding_seq) {
      if (!just_check_key_exists) {
        val.data_ = file_data_.data_ + entry.valuePos;
        val.size_ = entry.valueLen;
      }
      else {
        if (kTypeMerge == pikey.type) {
          pikey.type = kTypeValue; // instruct SaveValue to stop earlier
        }
      }
      get_context->SaveValue(pikey, val, &matched, &noop_pinner);
    }
  }
  return st;
}

Status TopFastTableReader::VerifyChecksum(const ReadOptions&, TableReaderCaller) {
  return Status::OK();
}

// for gdb debug purpose
size_t DumpInternalIterator(InternalIterator* iter, size_t num, bool hex) {
  if (!iter->Valid()) {
    return 0;
  }
  std::string backup_key = iter->key().ToString();
  size_t i = 0;
  for (; i < num && iter->Valid(); ++i) {
    ROCKSDB_ASSERT_GE(iter->key().size(), kNumInternalBytes);
    ParsedInternalKey pikey(iter->key());
    fstring v = iter->value();
    fprintf(stderr, "%.*s : %8lld : %2d : %.*s\n",
            (int)pikey.user_key.size_, pikey.user_key.data_,
            (long long)pikey.sequence, pikey.type,
            v.ilen(), v.p);
    iter->Next();
  }
  iter->Seek(backup_key);
  return i;
}

class TopFastTableReader::BaseIter : public InternalIterator, boost::noncopyable {
public:
  const TopFastTableReader* tab_;
  const std::unique_ptr<MainPatricia>* cspp_;
  Patricia::Iterator* iter_;
  int trie_idx_;
  int trie_num_;
  int val_idx_;
  int val_num_;
  uint32_t val_pos_;
  uint32_t val_len_;
  const uint64_t* seq_arr_;
  const uint32_t* pos_arr_;

  explicit BaseIter(const TopFastTableReader* table) { // NOLINT
    tab_ = table;
    cspp_ = table->cspp_.data();
    iter_ = cspp_[0]->new_iter();
    trie_num_ = int(table->cspp_.size());
    SetInvalid();
  }
  ~BaseIter() override {
    iter_->dispose();
    as_atomic(tab_->live_iter_num_).fetch_sub(1, std::memory_order_relaxed);
  }
  void SetInvalid() {
    trie_idx_ = -1;
    val_idx_ = -1;
    val_num_ = 0;
    val_pos_ = UINT32_MAX;
    val_len_ = UINT32_MAX;
    seq_arr_ = nullptr;
    pos_arr_ = nullptr;
  }
  void SetPinnedItersMgr(PinnedIteratorsManager*) final {}
  bool Valid() const noexcept final { return -1 != val_idx_; }
  void SeekForPrevAux(const Slice& target, const InternalKeyComparator& c) {
    SeekForPrevImpl(target, &c);
  }
  void SeekForPrev(const Slice& target) final {
    if (tab_->isReverseBytewiseOrder_)
      SeekForPrevAux(target, InternalKeyComparator(ReverseBytewiseComparator()));
    else
      SeekForPrevAux(target, InternalKeyComparator(BytewiseComparator()));
  }
  void SetAtFirstValue(const MainPatricia* trie) {
    auto entry = iter_->value_of<TopFastIndexEntry>();
    unaligned_save(iter_->mutable_word().ensure_unused(8), entry.seqvt);
    if (entry.valueMul) {
      val_num_ = entry.valueLen;
      assert(val_num_ >= 2);
      pos_arr_ = (const uint32_t*)trie->mem_get(entry.valuePos);
      seq_arr_ = (const uint64_t*)(pos_arr_ + val_num_ + 1);
      val_pos_ = pos_arr_[0];
      val_len_ = pos_arr_[1] - pos_arr_[0];
    } else {
      val_num_ = 1;
      pos_arr_ = nullptr;
      seq_arr_ = nullptr;
      val_pos_ = entry.valuePos;
      val_len_ = entry.valueLen;
    }
    val_idx_ = 0;
  }
  void SetAtLastValue(const MainPatricia* trie) {
    auto entry = iter_->value_of<TopFastIndexEntry>();
    if (entry.valueMul) {
      val_num_ = entry.valueLen;
      assert(val_num_ >= 2);
      pos_arr_ = (const uint32_t*)trie->mem_get(entry.valuePos);
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
    }
    unaligned_save(iter_->mutable_word().ensure_unused(8), entry.seqvt);
  }
  void SeekSeq(const MainPatricia* trie, uint64_t seq) {
    auto entry = iter_->value_of<TopFastIndexEntry>();
    if (entry.valueMul) {
      size_t vnum = entry.valueLen;
      val_num_ = entry.valueLen;
      assert(val_num_ >= 2);
      pos_arr_ = (const uint32_t*)trie->mem_get(entry.valuePos);
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
      if ((entry.seqvt >> 8) <= seq)
        unaligned_save(iter_->mutable_word().ensure_unused(8), entry.seqvt);
      else
        Next();
    }
  }
  Slice key() const noexcept final {
    TERARK_ASSERT_GE(val_idx_, 0);
    TERARK_ASSERT_LT(val_idx_, val_num_);
    fstring word = iter_->word();
    TERARK_ASSERT_GE(iter_->mutable_word().capacity(), word.size() + 8);
    return Slice(word.data(), word.size() + 8); // NOLINT
  }
  Slice user_key() const noexcept final {
    TERARK_ASSERT_GE(val_idx_, 0);
    TERARK_ASSERT_LT(val_idx_, val_num_);
    fstring word = iter_->word();
    TERARK_ASSERT_GE(iter_->mutable_word().capacity(), word.size() + 8);
    return Slice(word.data(), word.size()); // NOLINT
  }
  Slice value() const noexcept final {
    TERARK_ASSERT_GE(val_idx_, 0);
    TERARK_ASSERT_LT(val_idx_, val_num_);
    return Slice(tab_->file_data_.data_ + val_pos_, val_len_); // NOLINT
  }
  void Next() noexcept override = 0;
  void Prev() noexcept override = 0;
  bool NextAndGetResult(IterateResult* result) noexcept final {
    Next();
    if (this->Valid()) {
      result->SetKey(this->key());
      result->bound_check_result = IterBoundCheck::kUnknown;
      result->value_prepared = true;
      return true;
    }
    return false;
  }
  Status status() const final { return Status::OK(); }
  bool IsKeyPinned() const final { return false; }
  bool IsValuePinned() const final { return true; }
};
class TopFastTableReader::Iter : public BaseIter {
public:
  using BaseIter::BaseIter;
  void SeekToFirst() final {
    trie_idx_ = 0;
    auto first_trie = cspp_[0].get();
    if (iter_->trie() != first_trie) {
      iter_->reset(first_trie, initial_state);
    }
    TERARK_VERIFY(iter_->seek_begin());
    SetAtFirstValue(first_trie);
  }
  void SeekToLast() final {
    trie_idx_ = trie_num_ - 1;
    auto last_trie = cspp_[trie_idx_].get();
    if (iter_->trie() != last_trie) {
      iter_->reset(last_trie, initial_state);
    }
    TERARK_VERIFY(iter_->seek_end());
    SetAtLastValue(last_trie);
  }
  void Seek(const Slice& target) final {
    ROCKSDB_ASSERT_GE(target.size(), kNumInternalBytes);
    ParsedInternalKey pikey(target);
    size_t lo = tab_->offset_info_.prefixSet_.lower_bound_prefix(pikey.user_key);
    if (UNLIKELY(lo >= size_t(trie_num_))) {
      SetInvalid();
      return;
    }
    trie_idx_ = int(lo);
    auto trie = cspp_[lo].get();
    if (iter_->trie() != trie) {
      iter_->reset(trie, initial_state);
    }
    if (UNLIKELY(!iter_->seek_lower_bound(pikey.user_key))) {
      if (lo + 1 == size_t(trie_num_)) {
        SetInvalid();
        return;
      }
      trie_idx_ = int(lo+1);
      trie = cspp_[lo+1].get();
      iter_->reset(trie, initial_state);
      TERARK_VERIFY(iter_->seek_begin());
    }
    if (iter_->word() == pikey.user_key)
      SeekSeq(trie, pikey.sequence);
    else
      SetAtFirstValue(trie);
  }
  void Next() noexcept final {
    TERARK_ASSERT_GE(val_idx_, 0);
    TERARK_ASSERT_LE(val_idx_, val_num_);
    if (++val_idx_ < val_num_) {
      assert(val_num_ >= 2);
      auto seqvt = unaligned_load<uint64_t>(seq_arr_, val_idx_-1);
      unaligned_save(iter_->mutable_word().end(), seqvt);
      val_pos_ = pos_arr_[val_idx_];
      val_len_ = pos_arr_[val_idx_ + 1] - val_pos_;
      return;
    }
    auto trie = static_cast<const MainPatricia*>(iter_->trie()); // NOLINT
    if (UNLIKELY(!iter_->incr())) {
      if (++trie_idx_ >= trie_num_) {
        SetInvalid();
        return;
      }
      trie = cspp_[trie_idx_].get();
      iter_->reset(trie);
      TERARK_VERIFY(iter_->seek_begin());
    }
    SetAtFirstValue(trie);
  }
  void Prev() noexcept final {
    TERARK_ASSERT_GE(val_idx_, 0);
    TERARK_ASSERT_LE(val_idx_, val_num_);
    if (--val_idx_ >= 0) {
      uint64_t seqvt = val_idx_ > 0
                     ? unaligned_load<uint64_t>(seq_arr_, val_idx_-1)
                     : iter_->value_of<TopFastIndexEntry>().seqvt;
      unaligned_save(iter_->mutable_word().end(), seqvt);
      val_pos_ = pos_arr_[val_idx_];
      val_len_ = pos_arr_[val_idx_ + 1] - val_pos_;
      return;
    }
    auto trie = static_cast<const MainPatricia*>(iter_->trie()); // NOLINT
    if (UNLIKELY(!iter_->decr())) {
      if (--trie_idx_ < 0) {
        SetInvalid();
        return;
      }
      trie = cspp_[trie_idx_].get();
      iter_->reset(trie);
      TERARK_VERIFY(iter_->seek_end());
    }
    SetAtLastValue(trie);
  }
};
class TopFastTableReader::RevIter : public BaseIter {
public:
  using BaseIter::BaseIter;
  void SeekToFirst() final {
    trie_idx_ = 0;
    auto first_trie = cspp_[0].get();
    if (iter_->trie() != first_trie) {
      iter_->reset(first_trie, initial_state);
    }
    TERARK_VERIFY(iter_->seek_end());
    SetAtFirstValue(first_trie);
  }
  void SeekToLast() final {
    trie_idx_ = trie_num_ - 1;
    auto last_trie = cspp_[trie_idx_].get();
    if (iter_->trie() != last_trie) {
      iter_->reset(last_trie, initial_state);
    }
    TERARK_VERIFY(iter_->seek_begin());
    SetAtLastValue(last_trie);
  }
  void Seek(const Slice& target) final {
    ROCKSDB_ASSERT_GE(target.size(), kNumInternalBytes);
    ParsedInternalKey pikey(target);
    size_t lo = tab_->offset_info_.prefixSet_.upper_bound(pikey.user_key);
    if (UNLIKELY(0 == lo)) { // ReverseBytewise end
      SetInvalid();
      return;
    }
    lo = tab_->offset_info_.prefixSet_.size() - lo; // reverse
    trie_idx_ = int(lo);
    auto trie = cspp_[lo].get();
    if (iter_->trie() != trie) {
      iter_->reset(trie, initial_state);
    }
    if (iter_->seek_lower_bound(pikey.user_key)) {
      if (iter_->word() != pikey.user_key && !iter_->decr()) {
        // reach ReverseBytewise end of curr trie
        if (lo + 1 == size_t(trie_num_)) {
          SetInvalid();
          return;
        }
        trie_idx_ = int(lo+1);
        trie = cspp_[lo+1].get();
        iter_->reset(trie, initial_state);
        TERARK_VERIFY(iter_->seek_end());
      }
    }
    else { // ReverseBytewise begin of curr trie
      TERARK_VERIFY(iter_->seek_end());
    }
    // now iter is ReverseBytewise lower_bound
    if (iter_->word() == pikey.user_key)
      SeekSeq(trie, pikey.sequence);
    else
      SetAtFirstValue(trie);
  }
  void Next() noexcept final {
    TERARK_ASSERT_GE(val_idx_, 0);
    TERARK_ASSERT_LT(val_idx_, val_num_);
    if (++val_idx_ < val_num_) {
      assert(val_num_ >= 2);
      auto seqvt = unaligned_load<uint64_t>(seq_arr_, val_idx_-1);
      unaligned_save(iter_->mutable_word().end(), seqvt);
      val_pos_ = pos_arr_[val_idx_];
      val_len_ = pos_arr_[val_idx_ + 1] - val_pos_;
      return;
    }
    auto trie = static_cast<const MainPatricia*>(iter_->trie()); // NOLINT
    if (UNLIKELY(!iter_->decr())) {
      if (++trie_idx_ >= trie_num_) {
        SetInvalid();
        return;
      }
      trie = cspp_[trie_idx_].get();
      iter_->reset(trie);
      TERARK_VERIFY(iter_->seek_end());
    }
    SetAtFirstValue(trie);
  }
  void Prev() noexcept final {
    TERARK_ASSERT_GE(val_idx_, 0);
    TERARK_ASSERT_LT(val_idx_, val_num_);
    if (--val_idx_ >= 0) {
      uint64_t seqvt = val_idx_ > 0
                     ? unaligned_load<uint64_t>(seq_arr_, val_idx_-1)
                     : iter_->value_of<TopFastIndexEntry>().seqvt;
      unaligned_save(iter_->mutable_word().end(), seqvt);
      val_pos_ = pos_arr_[val_idx_];
      val_len_ = pos_arr_[val_idx_ + 1] - val_pos_;
      return;
    }
    auto trie = static_cast<const MainPatricia*>(iter_->trie()); // NOLINT
    if (UNLIKELY(!iter_->incr())) {
      if (--trie_idx_ < 0) {
        SetInvalid();
        return;
      }
      trie = cspp_[trie_idx_].get();
      iter_->reset(trie);
      TERARK_VERIFY(iter_->seek_begin());
    }
    SetAtLastValue(trie);
  }
};
InternalIterator*
TopFastTableReader::NewIterator(const ReadOptions& ro,
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

bool TopFastTableReader::GetRandomInteranlKeysAppend(
                  size_t num, std::vector<std::string>* output) const {
  size_t oldsize = output->size();
  size_t avgnum = ceiled_div(num, cspp_.size());
  SortableStrVec keys;
  for (auto& trie : cspp_) {
    trie->dfa_get_random_keys(&keys, avgnum);
    MainPatricia::SingleReaderToken token(trie.get());
    for (size_t i = 0; i < keys.size(); ++i) {
      fstring onekey = keys[i];
      output->push_back(FirstInternalKey(SliceOf(onekey), token));
    }
  }
  auto beg = output->begin() + oldsize;
  auto end = output->end();
  if (size_t(end - beg) > num) {
    size_t seed = output->size() + cspp_[0]->total_states()
                + size_t(beg->data()) + size_t(end[-1].data());
    std::shuffle(beg, end, std::mt19937(seed));
    output->resize(oldsize + num);
  }
  return true;
}

std::string TopFastTableReader::FirstInternalKey(
        Slice user_key, MainPatricia::SingleReaderToken& token) const {
  TERARK_VERIFY(token.trie()->lookup(user_key, &token));
  auto entry = token.value_of<TopFastIndexEntry>();
  std::string ikey;
  ikey.reserve(user_key.size_ + 8);
  ikey.append(user_key.data_, user_key.size_);
  ikey.append((char*)&entry.seqvt, 8);
  return ikey;
}

std::string TopFastTableReader::ToWebViewString(const json& dump_options) const {
  json djs;
  auto& props = *table_properties_;
  djs["Props.User"] = TableUserPropsToString(props.user_collected_properties, dump_options);
  djs["PrefixLen"] = offset_info_.prefixSet_.m_fixlen;
  djs["num_entries"] = props.num_entries;
  djs["num_deletions"] = props.num_deletions;
  djs["num_merges"] = props.num_merge_operands;
  djs["num_range_del"] = props.num_range_deletions;
  djs["data"] = {
    {"size", props.data_size},
    {"avg", props.data_size / double(props.num_entries) },
  };
  djs["raw"] = {
    {"key_size", props.raw_key_size},
    {"value_size", props.raw_value_size}
  };
  djs["parts"] = cspp_.size();
  djs["parts[0]"] = {{"cspp", {
      {"size", SizeToString(cspp_[0]->mem_size_inline())},
      {"zpath_states", cspp_[0]->num_zpath_states()},
    }}, { "value", {
      {"offset", offset_info_.offset_[0].offset},
      {"size", offset_info_.offset_[0].value},
    }},
  };
  return JsonToString(djs, dump_options);
}

/////////////////////////////////////////////////////////////////////////////

void TopFastTableReader::Open(RandomAccessFileReader* file, Slice file_data,
                              const TableReaderOptions& tro,
                              WarmupLevel warmupLevel) {
  uint64_t file_size = file_data.size_;
  try {
    LoadCommonPart(file, tro, file_data, kTopFastTableMagic);
  }
  catch (const Status&) { // very rare, try EmptyTable
    BlockContents emptyTableBC = ReadMetaBlockE(
        file, file_size, kTopEmptyTableMagicNumber,
        tro.ioptions, kTopEmptyTableKey);
    TERARK_VERIFY(!emptyTableBC.data.empty());
    INFO(tro.ioptions.info_log,
         "TopFastTableReader::Open: %s is EmptyTable, it's ok\n",
         file->file_name().c_str());
    auto t = UniquePtrOf(new TopEmptyTableReader());
    file_.release(); // NOLINT
    t->Open(file, file_data, tro);
    throw t.release(); // NOLINT
  }
  // defined in top_fast_table_builder.cc
  extern const std::string kTopFastTableOffsetBlock;
  BlockContents offsetBlock = ReadMetaBlockE(file, file_size,
        kTopFastTableMagic, tro.ioptions, kTopFastTableOffsetBlock);
  if (!offset_info_.risk_set_memory(offsetBlock.data)) {
    throw Status::Corruption(ROCKSDB_FUNC, "offset_info_.risk_set_memory()");
  }
  TERARK_VERIFY_EQ(offset_info_.prefixSet_.size()+1, offset_info_.offset_.size());
  cspp_.resize(offset_info_.prefixSet_.size());
  index_size_ = 0;
  live_iter_num_ = 0;
  for (size_t i = 0; i < cspp_.size(); ++i) {
    auto& x = offset_info_.offset_[i];
    TERARK_VERIFY_AL(x.offset, 64);
    TERARK_VERIFY_AL(x.value , 64);
    auto mem = x.offset + x.value + file_data_.data_ ;
    auto len = x.key;
    if (tro.ioptions.advise_random_on_open) {
      if (warmupLevel < WarmupLevel::kValue)
        MmapAdvRnd(file_data_.data_ + x.offset, x.value);
    }
    if (WarmupLevel::kIndex == warmupLevel) {
      MmapWarmUp(mem, len);
    }
    auto dfa = UniquePtrOf(BaseDFA::load_mmap_user_mem(mem, len));
    auto trie = dynamic_cast<MainPatricia*>(dfa.get());
    if (!trie) {
      auto header = reinterpret_cast<const ToplingIndexHeader*>(mem);
      throw Status::Corruption(ROCKSDB_FUNC,
                std::string("dfa is not MainPatricia, but is: ") +
                  header->magic + " : " + header->class_name);
    }
    dfa.release(); // NOLINT
    cspp_[i].reset(trie);
    index_size_ += len;
  }
}

TopFastTableReader::~TopFastTableReader() {
  TERARK_VERIFY_F(0 == live_iter_num_, "real: %zd", live_iter_num_);
  //use compiler auto destruct sequence, destruct iter_cache_ before cspp_
  //iter_cache_.~ThreadLocalPtr();
  //cspp_.clear();

  offset_info_.risk_release_ownership();
}

Status
TopFastTableFactory::NewTableReader(
            const ReadOptions& ro,
            const TableReaderOptions& tro,
            std::unique_ptr<RandomAccessFileReader>&& file,
            uint64_t file_size,
            std::unique_ptr<TableReader>* table,
            bool prefetch_index_and_filter_in_cache)
const try {
  (void)prefetch_index_and_filter_in_cache; // now ignore
  IOOptions ioopt;
  Slice file_data;
  file->exchange(new MmapReadWrapper(file));
 #if ROCKSDB_MAJOR < 7
  Status s = file->Read(ioopt, 0, file_size, &file_data, nullptr, nullptr);
 #else
  Status s = file->Read(ioopt, 0, file_size, &file_data, nullptr, nullptr, Env::IO_HIGH);
 #endif
  if (!s.ok()) {
    return s;
  }
  if (WarmupLevel::kValue == table_options_.warmupLevel) {
    MmapAdvSeq(file_data);
    MmapWarmUp(file_data);
  }
  auto t = new TopFastTableReader();
  table->reset(t);
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

///////////////////////////////////////////////////////////////////////////
struct TopTableReader_Manip : PluginManipFunc<TableReader> {
  void Update(TableReader*, const json&, const json& js,
              const SidePluginRepo& repo) const final {
    THROW_NotSupported("Reader is read only");
  }
  std::string ToString(const TableReader& reader, const json& dump_options,
                       const SidePluginRepo& repo) const final {
    if (auto rd = dynamic_cast<const TopTableReaderBase*>(&reader)) {
      return rd->ToWebViewString(dump_options);
    }
    THROW_InvalidArgument("Unknow TableReader");
  }
};
// Register as compression algo name
ROCKSDB_REG_PluginManip("FlatZip", TopTableReader_Manip);
ROCKSDB_REG_PluginManip("TooZip", TopTableReader_Manip);
ROCKSDB_REG_PluginManip("TooFast", TopTableReader_Manip);
ROCKSDB_REG_PluginManip("SngFast", TopTableReader_Manip);


} // ROCKSDB_NAMESPACE
