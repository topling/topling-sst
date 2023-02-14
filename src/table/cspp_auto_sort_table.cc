//
// Created by leipeng on 2022-06-16 17:30
//
#include <rocksdb/table.h>
#include <monitoring/iostats_context_imp.h>
#include <topling/side_plugin_factory.h>
#include <topling/builtin_table_factory.h>

#include <logging/logging.h>
#include <table/meta_blocks.h>
#include <terark/fsa/cspptrie.inl>

#include "top_table_reader.h"
#include "top_table_builder.h"

#include <terark/num_to_str.hpp>
#include <terark/util/sortable_strvec.hpp>

namespace rocksdb {

using namespace terark;

#pragma pack(push,4)
struct FixedValue {
  uint32_t idx; // used for ApproximateOffsetOf
  char data[0];
};
struct UniqEntry {
  // idx as first member is for compatible with FixedValue
  // thus implementation for ApproximateOffsetOf can be same
  uint32_t idx; // used for ApproximateOffsetOf
  uint32_t vlen;
  uint64_t offset : 36; // 64G
  uint64_t type   :  7;
  uint64_t klen   : 21; //  2M
};
struct UniqEntryNoRank {
  uint32_t vlen;
  uint64_t offset : 36; // 64G
  uint64_t type   :  7;
  uint64_t klen   : 21; //  2M
};
#pragma pack(pop)
static_assert(sizeof(UniqEntry) == 16);

const uint64_t kCSPPAutoSortTableMagic = 0x74726f536f747541ULL; // AutoSort
extern const std::string kCSPPIndex;

class CSPPAutoSortTableFactory : public TableFactory {
public:
  CSPPAutoSortTableFactory(const json& js, const SidePluginRepo& repo) {
    Update({}, js, repo);
  }
  ~CSPPAutoSortTableFactory() override;

  void Update(const json&, const json&, const SidePluginRepo&);
  std::string ToString(const json& dump_options, const SidePluginRepo&) const;

  const char* Name() const override { return "CSPPAutoSortTable"; }

  using TableFactory::NewTableReader;
  Status
  NewTableReader(const ReadOptions&,
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

  bool SupportAutoSort() const final { return true; }

  size_t fileWriteBufferSize = 32*1024;
  bool enableIndexRank = true;
  bool forceNeedCompact = true;

  // stats
  mutable std::mutex mtx;
  mutable gold_hash_map<int, int> fixed_value_stats;
  mutable long long start_time_point_ = 0;
  mutable long long build_time_duration_ = 0; // exclude idle time
  mutable size_t num_writers_ = 0;
  mutable size_t num_readers_ = 0;
  mutable size_t sum_user_key_len_ = 0;
  mutable size_t sum_user_key_cnt_ = 0;
  mutable size_t sum_value_len_ = 0;
  mutable size_t sum_index_len_ = 0;
};

class CSPPAutoSortTableBuilder : public TopTableBuilderBase {
public:
  CSPPAutoSortTableBuilder(const CSPPAutoSortTableFactory*, const TableBuilderOptions&,
                       WritableFileWriter* file);
  ~CSPPAutoSortTableBuilder() { if (!closed_) wtoken_.release(); }
  void Add(const Slice& key, const Slice& value) final;
  Status GetBoundaryUserKey(std::string*, std::string*) const final;
  Status Finish() final;
  void Abandon() final;
  uint64_t EstimatedFileSize() const final { return offset_ + cspp_.mem_size_inline(); }
  void DoWriteAppend(const void* data, size_t size) {
    fobuf_.ensureWrite(data, size);
    offset_ += size;
  }
  void ToplingFlushBuffer();
  bool NeedCompact() const final;
  bool IsEnableIndexRank() const { return !properties_.index_value_is_delta_encoded; }

  const CSPPAutoSortTableFactory* table_factory_;
  MainPatricia cspp_;
  MainPatricia::SingleWriterToken wtoken_;
  OsFileStream fstream_;
  OutputBuffer fobuf_;
  long long t0 = 0;
};

static inline
size_t TrieValueSize(int sst_fixed_value_len, bool enableIndexRank) {
  if (enableIndexRank) {
    if (sst_fixed_value_len >= 0)
      return sizeof(uint32_t) + sst_fixed_value_len;
    else
      return sizeof(UniqEntry);
  } else {
    if (sst_fixed_value_len >= 0)
      return sst_fixed_value_len;
    else
      return sizeof(UniqEntryNoRank);
  }
}
CSPPAutoSortTableBuilder::CSPPAutoSortTableBuilder(
        const CSPPAutoSortTableFactory* table_factory,
        const TableBuilderOptions& tbo,
        WritableFileWriter* file)
  : TopTableBuilderBase(tbo, file)
  , table_factory_(table_factory)
  , cspp_(TrieValueSize(tbo.fixed_value_len, table_factory->enableIndexRank),
          2u<<20u, Patricia::SingleThreadStrict)
{
  if (tbo.fixed_value_len >= 0) {
    table_factory->mtx.lock();
    table_factory->fixed_value_stats[tbo.fixed_value_len]++;
    table_factory->mtx.unlock();
  }
  properties_.fixed_value_len = tbo.fixed_value_len;
  properties_.compression_name = "AutoSort";

  // use index_value_is_delta_encoded as enableIndexRank_ flag(inversed),
  // inverse enableIndexRank to compatible to old format, because old
  // format  enableIndexRank is always true
  properties_.index_value_is_delta_encoded = !table_factory->enableIndexRank;

  int fd = (int)file->writable_file()->FileDescriptor();
  TERARK_VERIFY_GE(fd, 0);
  fstream_.attach(fd);
  fobuf_.attach(&fstream_);
  fobuf_.initbuf(table_factory->fileWriteBufferSize);

  wtoken_.acquire(&cspp_);
  t0 = g_pf.now();
}

void CSPPAutoSortTableBuilder::Add(const Slice& key, const Slice& value) try {
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
    TERARK_VERIFY_LE(vt, 0x1F);
    const fstring userKey(key.data(), key.size() - 8);
    if (int(properties_.fixed_value_len) >= 0) {
      ROCKSDB_VERIFY_EQ(value.size(), properties_.fixed_value_len);
      ROCKSDB_VERIFY_EQ(vt, kTypeValue);
      void* pv;
      if (IsEnableIndexRank()) {
        auto fv = (FixedValue*)alloca(4 + value.size_);
        fv->idx = uint32_t(num_user_key_);
        memcpy(fv->data, value.data_, value.size_);
        pv = fv;
      } else {
        pv = alloca(value.size_);
        memcpy(pv, value.data_, value.size_);
      }
      if (!cspp_.insert(userKey, pv, &wtoken_)) {
        status_ = Status::InvalidArgument("Duplicate UserKey", value.ToString(true));
      }
    }
    else {
      auto insert = [&,this](auto&& ue) {
        ue.vlen = uint32_t(value.size());
        ue.offset = offset_;
        ue.type = vt;
        ue.klen = userKey.size();
        if (!cspp_.insert(userKey, &ue, &wtoken_)) {
          status_ = Status::InvalidArgument("Duplicate UserKey", value.ToString(true));
          return;
        }
      };
      if (IsEnableIndexRank()) {
        insert(UniqEntry{uint32_t(num_user_key_)});
      } else {
        insert(UniqEntryNoRank{});
      }
      DoWriteAppend(value.data_, value.size_);
    }
    num_user_key_++;
  }
  else if (vt == kTypeRangeDeletion) {
    range_del_block_.Add(key, value);
  }
  else {
    const char* ename = enum_name(vt).data();
    // use full ename such as: "kTypeLogData = 0x3"
    TERARK_DIE("unexpected ValueType = %s(%d)", ename, vt);
  }
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

Status CSPPAutoSortTableBuilder::Finish() try {
  if (0 == num_user_key_) {
    FinishAsEmptyTable();
    return Status::OK();
  }

  //---- Write CSPP Index
  wtoken_.release();
  ROCKSDB_VERIFY_EQ(num_user_key_, cspp_.num_words());
  cspp_.set_readonly();
  uint64_t estimate_fsize = offset_ + cspp_.mem_size_inline();
  uint64_t avg_len = estimate_fsize / num_user_key_;
  Patricia::IteratorPtr iter(cspp_.new_iter());
  bool hasNext = iter->seek_begin();
  auto log = ioptions_.info_log.get();
  if (IsEnableIndexRank()) {
if (int(properties_.fixed_value_len) >= 0) {
  uint64_t idx;
  for (idx = 0; hasNext; hasNext = iter->incr(), idx++) {
    auto& ue = iter->mutable_value_of<FixedValue>();
    auto& uk = iter->mutable_word();
    ue.idx = uint32_t(idx); // sorting order
    uk.ensure_unused(8);
    unaligned_save<uint64_t>(uk.end(), PackSequenceAndType(0, kTypeValue));
    const Slice ikey((const char*)uk.data(), uk.size() + 8);
    uint64_t estimate_offset = avg_len * idx;
    Slice val(ue.data, size_t(properties_.fixed_value_len));
    NotifyCollectTableCollectorsOnAdd(ikey, val, estimate_offset, collectors_, log);
  }
  ROCKSDB_VERIFY_EQ(idx, cspp_.num_words());
}
else {
  uint64_t idx;
  for (idx = 0; hasNext; hasNext = iter->incr(), idx++) {
    auto& ue = iter->mutable_value_of<UniqEntry>();
    auto& uk = iter->mutable_word();
    ue.idx = uint32_t(idx); // sorting order
    uk.ensure_unused(8);
    unaligned_save<uint64_t>(uk.end(), PackSequenceAndType(0, ValueType(ue.type)));
    const Slice ikey((const char*)uk.data(), uk.size() + 8);
    uint64_t estimate_offset = avg_len * idx;
    Slice val;
    val.data_ = nullptr; // now myrocks just use val.size
    val.size_ = ue.vlen;
    NotifyCollectTableCollectorsOnAdd(ikey, val, estimate_offset, collectors_, log);
  }
  ROCKSDB_VERIFY_EQ(idx, cspp_.num_words());
}
  } // if (IsEnableIndexRank())

  using namespace std::placeholders;
  // NOLINTNEXTLINE
  auto writeAppend = std::bind(&CSPPAutoSortTableBuilder::DoWriteAppend, this, _1, _2);
  Padzero<64>(writeAppend, offset_);
  size_t indexOffset = offset_;
  cspp_.save_mmap(writeAppend);
  //---- End Write CSPP Index
  size_t index_size = offset_ - indexOffset;
  properties_.index_size = index_size;
  properties_.data_size  = indexOffset;
  properties_.num_data_blocks = 1;

  ToplingFlushBuffer();
  WriteMeta(kCSPPAutoSortTableMagic, {{kCSPPIndex, {indexOffset, index_size}}});

  auto fac = table_factory_;
  auto ukey_len = properties_.raw_key_size - 8 * properties_.num_entries;
  long long t1 = g_pf.now();
  auto td = g_pf.us(t1 - t0);
  as_atomic(fac->sum_user_key_cnt_).fetch_add(num_user_key_, std::memory_order_relaxed);
  as_atomic(fac->sum_user_key_len_).fetch_add(ukey_len, std::memory_order_relaxed);
  as_atomic(fac->sum_value_len_).fetch_add(properties_.raw_value_size, std::memory_order_relaxed);
  as_atomic(fac->sum_index_len_).fetch_add(index_size, std::memory_order_relaxed);
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

Status CSPPAutoSortTableBuilder::GetBoundaryUserKey(std::string* smallest,
                                                std::string* largest) const {
  if (isReverseBytewiseOrder_) {
    std::swap(smallest, largest);
  }
  Patricia::IteratorPtr iter(cspp_.new_iter());
  ROCKSDB_VERIFY(iter->seek_begin());
  smallest->assign(iter->word().str());
  ROCKSDB_VERIFY(iter->seek_end());
  largest->assign(iter->word().str());
  return Status::OK();
}

void CSPPAutoSortTableBuilder::Abandon() {
  wtoken_.release();
  fobuf_.resetbuf(); // discard buffer content
  ToplingFlushBuffer();
  closed_ = true;
}

void CSPPAutoSortTableBuilder::ToplingFlushBuffer() {
  fobuf_.flush_buffer();
  fstream_.detach();
  file_->SetFileSize(offset_); // fool the WritableFileWriter
  file_->writable_file()->SetFileSize(offset_); // fool the WritableFile
  IOSTATS_ADD(bytes_written, offset_);
}

bool CSPPAutoSortTableBuilder::NeedCompact() const {
  if (table_factory_->forceNeedCompact) {
    return true;
  }
  return TopTableBuilderBase::NeedCompact();
}

TableBuilder*
CSPPAutoSortTableFactory::NewTableBuilder(const TableBuilderOptions& tbo,
                                        WritableFileWriter* file)
const {
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
  return new CSPPAutoSortTableBuilder(this, tbo, file);
}

class CSPPAutoSortTableReader : public TopTableReaderBase {
public:
  using TopTableReaderBase::TopTableReaderBase;
  ~CSPPAutoSortTableReader() override;
  void Open(RandomAccessFileReader*, Slice file_data, const TableReaderOptions&);
  InternalIterator*
  NewIterator(const ReadOptions&, const SliceTransform* prefix_extractor,
              Arena* arena, bool skip_filters, TableReaderCaller caller,
              size_t compaction_readahead_size,
              bool allow_unprepared_value) final;

  uint64_t ApproximateOffsetOf_impl(const Slice& key, Patricia::Iterator*);
  uint64_t ApproximateOffsetOf(const Slice& key, TableReaderCaller) final;
  uint64_t ApproximateSize(const Slice&, const Slice&, TableReaderCaller) final;
  size_t ApproximateMemoryUsage() const final { return file_data_.size_; }
  Status Get(const ReadOptions& readOptions, const Slice& key,
                     GetContext* get_context,
                     const SliceTransform* prefix_extractor,
                     bool skip_filters) final;

  Status VerifyChecksum(const ReadOptions&, TableReaderCaller) final;
  bool GetRandomInteranlKeysAppend(size_t num, std::vector<std::string>* output) const final;
  std::string FirstInternalKey(Slice user_key, MainPatricia::SingleReaderToken&) const;
  std::string ToWebViewString(const json& dump_options) const final;

// data member also public
  MainPatricia* cspp_ = nullptr;
  size_t index_offset_; // also sum_value_len
  size_t index_size_;
  int fixed_value_len_;
  bool enableIndexRank_;

  class Iter;
  class BaseIter;
  class RevIter;
};

uint64_t CSPPAutoSortTableReader::ApproximateOffsetOf_impl(const Slice& ikey, Patricia::Iterator* iter) {
  TERARK_VERIFY_GE(ikey.size(), 8);
  assert(enableIndexRank_);
  // we ignore seqnum of ikey
  fstring user_key(ikey.data(), ikey.size() - 8);
  if (isReverseBytewiseOrder_) {
    if (iter->seek_rev_lower_bound(user_key)) {
      auto ue = iter->value_of<UniqEntry>();
      auto idx = cspp_->num_words() - 1 - ue.idx;
      return file_data_.size_ * idx / cspp_->num_words();
    }
  }
  else {
    if (iter->seek_lower_bound(user_key)) {
      auto ue = iter->value_of<UniqEntry>();
      return file_data_.size_ * ue.idx / cspp_->num_words();
    }
  }
  return file_data_.size_;
}
uint64_t CSPPAutoSortTableReader::ApproximateOffsetOf(const Slice& ikey, TableReaderCaller) {
  if (!enableIndexRank_)
    return 0;
  Patricia::IteratorPtr iter(cspp_->new_iter());
  return ApproximateOffsetOf_impl(ikey, iter.get());
}
uint64_t CSPPAutoSortTableReader::ApproximateSize(const Slice& beg, const Slice& end,
                                              TableReaderCaller) {
  if (!enableIndexRank_)
    return 0;
  Patricia::IteratorPtr iter(cspp_->new_iter());
  uint64_t offset_beg = ApproximateOffsetOf_impl(beg, iter.get());
  uint64_t offset_end = ApproximateOffsetOf_impl(end, iter.get());
  return offset_end - offset_beg;
}

Status CSPPAutoSortTableReader::Get(const ReadOptions& read_options,
                                const Slice& ikey,
                                GetContext* get_context,
                                const SliceTransform* /*prefix_extractor*/,
                                bool /*skip_filters*/) {
  ROCKSDB_ASSERT_GE(ikey.size(), kNumInternalBytes);
  ParsedInternalKey pikey(ikey);
  if (global_seqno_ <= pikey.sequence) {
    MainPatricia::SingleReaderToken token(cspp_);
    if (cspp_->lookup(pikey.user_key, &token)) {
      Slice val;
      if (!read_options.just_check_key_exists) {
        if (fixed_value_len_ >= 0) {
          if (enableIndexRank_)
            val.data_ = token.mutable_value_of<FixedValue>().data;
          else
            val.data_ = (const char*)token.value();
          val.size_ = fixed_value_len_;
        }
        else if (enableIndexRank_) {
          auto ue = token.value_of<UniqEntry>();
          val.data_ = file_data_.data_ + ue.offset;
          val.size_ = ue.vlen;
        }
        else {
          auto ue = token.value_of<UniqEntryNoRank>();
          val.data_ = file_data_.data_ + ue.offset;
          val.size_ = ue.vlen;
        }
      } else {
        if (kTypeMerge == pikey.type)
          pikey.type = kTypeValue; // instruct SaveValue to stop earlier
      }
      Cleanable noop_pinner;
      get_context->SaveValue(pikey, val, &noop_pinner);
    }
  }
  return Status::OK();
}

Status CSPPAutoSortTableReader::VerifyChecksum(const ReadOptions&, TableReaderCaller) {
  return Status::OK();
}

class CSPPAutoSortTableReader::BaseIter : public InternalIterator, boost::noncopyable {
public:
  const CSPPAutoSortTableReader* tab_;
  MainPatricia* cspp_;
  Patricia::Iterator* iter_;
  uint64_t global_seqno_;
  int fixed_value_len_;
  bool is_valid_ = false;
  bool enableIndexRank_;
  explicit BaseIter(const CSPPAutoSortTableReader* table) { // NOLINT
    tab_ = table;
    cspp_ = table->cspp_;
    iter_ = table->cspp_->new_iter();
    global_seqno_ = table->global_seqno_;
    fixed_value_len_ = table->fixed_value_len_;
    enableIndexRank_ = table->enableIndexRank_;
  }
  ~BaseIter() override {
    iter_->dispose();
    as_atomic(tab_->live_iter_num_).fetch_sub(1, std::memory_order_relaxed);
  }
  void SetSeqVT(bool is_valid) {
    is_valid_ = is_valid;
    if (is_valid) {
      auto& uk = iter_->mutable_word();
      uk.ensure_unused(8);
      if (fixed_value_len_ >= 0) {
        uint64_t seqvt = PackSequenceAndType(global_seqno_, kTypeValue);
        unaligned_save<uint64_t>(uk.end(), seqvt);
      } else {
        auto  ue = iter_->value_of<UniqEntry>();
        uint64_t seqvt = PackSequenceAndType(global_seqno_, ValueType(ue.type));
        unaligned_save<uint64_t>(uk.end(), seqvt);
      }
    }
  }
  void SetPinnedItersMgr(PinnedIteratorsManager*) final {}
  bool Valid() const final { return is_valid_; }
  void SeekForPrevAux(const Slice& target, const InternalKeyComparator& c) {
    SeekForPrevImpl(target, &c);
  }
  void SeekForPrev(const Slice& target) final {
    if (tab_->isReverseBytewiseOrder_)
      SeekForPrevAux(target, InternalKeyComparator(ReverseBytewiseComparator()));
    else
      SeekForPrevAux(target, InternalKeyComparator(BytewiseComparator()));
  }
  Slice key() const final {
    assert(is_valid_);
    fstring word = iter_->word();
    TERARK_ASSERT_GE(iter_->mutable_word().capacity(), word.size() + 8);
    return Slice(word.data(), word.size() + 8); // NOLINT
  }
  Slice user_key() const final {
    assert(is_valid_);
    fstring word = iter_->word();
    TERARK_ASSERT_GE(iter_->mutable_word().capacity(), word.size() + 8);
    return Slice(word.data(), word.size()); // NOLINT
  }
  Slice value() const final {
    assert(is_valid_);
    if (fixed_value_len_ >= 0) {
      if (enableIndexRank_)
        return {iter_->mutable_value_of<FixedValue>().data, size_t(fixed_value_len_)};
      else
        return {(const char*)iter_->value(), size_t(fixed_value_len_)};
    }
    else {
      if (enableIndexRank_) {
        auto ue = iter_->value_of<UniqEntry>();
        return Slice(tab_->file_data_.data_ + ue.offset, ue.vlen); // NOLINT
      } else {
        auto ue = iter_->value_of<UniqEntryNoRank>();
        return Slice(tab_->file_data_.data_ + ue.offset, ue.vlen); // NOLINT
      }
    }
  }
  Status status() const final { return Status::OK(); }
  bool IsKeyPinned() const final { return false; }
  bool IsValuePinned() const final { return true; }
};
class CSPPAutoSortTableReader::Iter : public BaseIter {
public:
  using BaseIter::BaseIter;
  void SeekToFirst() final {
    TERARK_VERIFY(iter_->seek_begin());
    SetSeqVT(true);
  }
  void SeekToLast() final {
    TERARK_VERIFY(iter_->seek_end());
    SetSeqVT(true);
  }
  void Seek(const Slice& ikey) final {
    ROCKSDB_VERIFY_GE(ikey.size_, 8);
    fstring ukey(ikey.data_, ikey.size_-8);
    uint64_t seq;
    ValueType vt;
    UnPackSequenceAndType(unaligned_load<uint64_t>(ukey.end()), &seq, &vt);
    bool is_valid = iter_->seek_lower_bound(ukey);
    if (global_seqno_ > seq)
      is_valid = iter_->incr();
    SetSeqVT(is_valid);
  }
  void Next() final {
    assert(is_valid_);
    SetSeqVT(iter_->incr());
  }
  void Prev() final {
    assert(is_valid_);
    SetSeqVT(iter_->decr());
  }
  bool NextAndGetResult(IterateResult* result) final {
    Next();
    if (this->Valid()) {
      result->SetKey(this->key());
      result->bound_check_result = IterBoundCheck::kUnknown;
      result->value_prepared = true;
      return true;
    }
    return false;
  }
};
class CSPPAutoSortTableReader::RevIter : public BaseIter {
public:
  using BaseIter::BaseIter;
  void SeekToFirst() final {
    TERARK_VERIFY(iter_->seek_end());
    SetSeqVT(true);
  }
  void SeekToLast() final {
    TERARK_VERIFY(iter_->seek_begin());
    SetSeqVT(true);
  }
  void Seek(const Slice& ikey) final {
    ROCKSDB_VERIFY_GE(ikey.size_, 8);
    fstring ukey(ikey.data_, ikey.size_-8);
    uint64_t seq;
    ValueType vt;
    UnPackSequenceAndType(unaligned_load<uint64_t>(ukey.end()), &seq, &vt);
    bool is_valid = iter_->seek_rev_lower_bound(ukey);
    if (tab_->global_seqno_ > seq)
      is_valid = iter_->decr();
    SetSeqVT(is_valid);
  }
  void Next() final {
    assert(is_valid_);
    SetSeqVT(iter_->decr());
  }
  void Prev() final {
    assert(is_valid_);
    SetSeqVT(iter_->incr());
  }
  bool NextAndGetResult(IterateResult* result) final {
    Next();
    if (this->Valid()) {
      result->SetKey(this->key());
      result->bound_check_result = IterBoundCheck::kUnknown;
      result->value_prepared = true;
      return true;
    }
    return false;
  }
};
InternalIterator*
CSPPAutoSortTableReader::NewIterator(const ReadOptions& ro,
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

bool CSPPAutoSortTableReader::GetRandomInteranlKeysAppend(
                  size_t num, std::vector<std::string>* output) const {
  SortableStrVec keys;
  cspp_->dfa_get_random_keys(&keys, num);
  MainPatricia::SingleReaderToken token(cspp_);
  for (size_t i = 0; i < keys.size(); ++i) {
    fstring onekey = keys[i];
    output->push_back(FirstInternalKey(SliceOf(onekey), token));
  }
  return true;
}

std::string CSPPAutoSortTableReader::FirstInternalKey(
        Slice user_key, MainPatricia::SingleReaderToken& token) const {
  TERARK_VERIFY(token.trie()->lookup(user_key, &token));
  auto ue = token.value_of<UniqEntry>();
  uint64_t seqvt = PackSequenceAndType(global_seqno_, ValueType(ue.type));
  std::string ikey;
  ikey.reserve(user_key.size_ + 8);
  ikey.append(user_key.data_, user_key.size_);
  ikey.append((char*)&seqvt, 8);
  return ikey;
}

std::string CSPPAutoSortTableReader::ToWebViewString(const json& dump_options) const {
  return "CSPPAutoSortTableReader::ToWebViewString: to be done";
}

/////////////////////////////////////////////////////////////////////////////

void CSPPAutoSortTableReader::Open(RandomAccessFileReader* file, Slice file_data, const TableReaderOptions& tro) {
  uint64_t file_size = file_data.size_;
  try {
    LoadCommonPart(file, tro, file_data, kCSPPAutoSortTableMagic);
  }
  catch (const Status&) { // very rare, try EmptyTable
    BlockContents emptyTableBC = ReadMetaBlockE(
        file, file_size, kTopEmptyTableMagicNumber,
        tro.ioptions, kTopEmptyTableKey);
    TERARK_VERIFY(!emptyTableBC.data.empty());
    INFO(tro.ioptions.info_log,
         "CSPPAutoSortTableReader::Open: %s is EmptyTable, it's ok\n",
         file->file_name().c_str());
    auto t = UniquePtrOf(new TopEmptyTableReader());
    file_.release(); // NOLINT
    t->Open(file, file_data, tro);
    throw t.release(); // NOLINT
  }
  BlockContents indexBlock = ReadMetaBlockE(file, file_size,
        kCSPPAutoSortTableMagic, tro.ioptions, kCSPPIndex);
  index_size_ = indexBlock.data.size_;
  index_offset_ = indexBlock.data.data_ - file_data_.data_;
  fixed_value_len_ = int(table_properties_->fixed_value_len);
  enableIndexRank_ = !table_properties_->index_value_is_delta_encoded;
  live_iter_num_ = 0;
  TERARK_VERIFY_AL(index_offset_, 64);
  auto dfa = UniquePtrOf(BaseDFA::load_mmap_user_mem(indexBlock.data));
  cspp_ = dynamic_cast<MainPatricia*>(dfa.get());
  if (!cspp_) {
    auto header = reinterpret_cast<const ToplingIndexHeader*>(indexBlock.data.data_);
    throw Status::Corruption(ROCKSDB_FUNC,
              std::string("dfa is not MainPatricia, but is: ") +
                header->magic + " : " + header->class_name);
  }
  TERARK_VERIFY_F(cspp_->get_valsize() == TrieValueSize(fixed_value_len_, enableIndexRank_)
      , "%zd %zd(%d, %d)", cspp_->get_valsize()
      , TrieValueSize(fixed_value_len_, enableIndexRank_)
      , fixed_value_len_, enableIndexRank_
      );
  dfa.release(); // NOLINT
}

CSPPAutoSortTableReader::~CSPPAutoSortTableReader() {
  TERARK_VERIFY_F(0 == live_iter_num_, "real: %zd", live_iter_num_);
  delete cspp_;
}

Status
CSPPAutoSortTableFactory::NewTableReader(
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
  bool populate = true;
  file->exchange(new MmapReadWrapper(file, populate));
 #if ROCKSDB_MAJOR < 7
  Status s = file->Read(ioopt, 0, file_size, &file_data, nullptr, nullptr);
 #else
  Status s = file->Read(ioopt, 0, file_size, &file_data, nullptr, nullptr, Env::IO_HIGH);
 #endif
  if (!s.ok()) {
    return s;
  }
  MmapAdvSeq(file_data);
//MmapWarmUp(file_data);
  auto t = new CSPPAutoSortTableReader();
  table->reset(t);
  t->Open(file.release(), file_data, tro);
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

void CSPPAutoSortTableFactory::Update(const json&, const json& js, const SidePluginRepo&) {
  ROCKSDB_JSON_OPT_SIZE(js, fileWriteBufferSize);
  ROCKSDB_JSON_OPT_PROP(js, enableIndexRank);
  ROCKSDB_JSON_OPT_PROP(js, forceNeedCompact);
}

CSPPAutoSortTableFactory::~CSPPAutoSortTableFactory() { // NOLINT
}

std::string CSPPAutoSortTableFactory::GetPrintableOptions() const {
  SidePluginRepo* repo = nullptr;
  return ToString({}, *repo);
}

Status
CSPPAutoSortTableFactory::ValidateOptions(const DBOptions& db_opts,
                                      const ColumnFamilyOptions& cf_opts)
const {
  if (!IsBytewiseComparator(cf_opts.comparator)) {
    return Status::InvalidArgument(ROCKSDB_FUNC, "comparator is not bytewise");
  }
  TERARK_VERIFY_EZ(cf_opts.comparator->timestamp_size());
  return Status::OK();
}

ROCKSDB_REG_Plugin("CSPPAutoSortTable", CSPPAutoSortTableFactory, TableFactory);
ROCKSDB_RegTableFactoryMagicNumber(kCSPPAutoSortTableMagic, "CSPPAutoSortTable");

std::string CSPPAutoSortTableFactory::ToString(const json &dump_options,
                                           const SidePluginRepo&) const {
  json djs;
  bool html = JsonSmartBool(dump_options, "html");
  ROCKSDB_JSON_SET_SIZE(djs, fileWriteBufferSize);
  ROCKSDB_JSON_SET_PROP(djs, enableIndexRank);
  ROCKSDB_JSON_SET_PROP(djs, forceNeedCompact);
  djs["num_writers"] = num_writers_;
  djs["num_readers"] = num_readers_;

  long long t8 = g_pf.now();
  double td = g_pf.uf(start_time_point_, t8);
  char buf[64];
#define ToStr(...) json(std::string(buf, snprintf(buf, sizeof(buf), __VA_ARGS__)))

  auto sumlen = sum_user_key_len_ + sum_value_len_;
  djs["Statistics"] = json::object({
    { "sum_user_key_len", SizeToString(sum_user_key_len_) },
    { "sum_user_key_cnt", SizeToString(sum_user_key_cnt_) },
    { "sum_value_len", SizeToString(sum_value_len_) },
    { "sum_index_len", SizeToString(sum_index_len_) },
    { "avg_index_len", ToStr("%.3f MB", sum_index_len_/1e6/num_writers_) },
    { "raw:avg_uval" , ToStr("%.3f", sum_value_len_/1.0/sum_user_key_cnt_) },
    { "raw:avg_ukey", ToStr("%.3f", sum_user_key_len_/1.0/sum_user_key_cnt_) },
    { "zip:avg_ukey", ToStr("%.3f", sum_index_len_/1.0/sum_user_key_cnt_) },
    { "index:zip/raw", ToStr("%.3f", sum_index_len_/1.0/sum_user_key_len_) },
    { "raw:key/all", ToStr("%.3f", sum_user_key_len_/1.0/sumlen) },
    { "raw:val/all", ToStr("%.3f", sum_value_len_/1.0/sumlen) },
    { "zip:key/all", ToStr("%.3f", sum_index_len_/1.0/(sum_index_len_ + sum_value_len_)) },
    { "zip:val/all", ToStr("%.3f", sum_value_len_/1.0/(sum_index_len_ + sum_value_len_)) },
    { "write_speed_all.-seq", ToStr("%.3f MB/s", sumlen / td) },
  });
  valvec<std::pair<int,int> > fixed_snap(fixed_value_stats.size(), valvec_reserve());
  mtx.lock();
  for (size_t i = 0; i < fixed_value_stats.end_i(); i++) {
    fixed_snap.push_back(fixed_value_stats.elem_at(i));
  }
  mtx.unlock();
  json& fvjs = djs["FixedValue"];
  for (auto& [len, cnt] : fixed_snap) {
    fvjs.push_back({{"Length", len}, {"Count", cnt}});
  }
  if (html && !fixed_snap.empty()) {
    fvjs[0]["<htmltab:col>"] = json::array({ "Length", "Count" });
  }
  JS_TopTable_AddVersion(djs, html);
  return JsonToString(djs, dump_options);
}

ROCKSDB_REG_EasyProxyManip("CSPPAutoSortTable", CSPPAutoSortTableFactory, TableFactory);
} // namespace rocksdb
