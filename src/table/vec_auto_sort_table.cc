//
// Created by leipeng on 2022-08-10
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

#include "top_table_reader.h"
#include "top_table_builder.h"

#include <terark/io/MemStream.hpp>
#include <terark/num_to_str.hpp>
#include <terark/util/atomic.hpp>
#include <terark/util/sortable_strvec.hpp>
#include <terark/util/fstrvec.hpp>

namespace rocksdb {

using namespace terark;

const uint64_t kVecAutoSortTableMagic = 0x74726f5341636556ULL; // VecASort
const std::string kStrVecIndex = "StrVecIndex";

class VecAutoSortTableFactory : public TableFactory {
public:
  VecAutoSortTableFactory(const json& js, const SidePluginRepo& repo) {
    Update({}, js, repo);
  }
  ~VecAutoSortTableFactory() override;

  void Update(const json&, const json&, const SidePluginRepo&);
  std::string ToString(const json& dump_options, const SidePluginRepo&) const;

  const char* Name() const override { return "VecAutoSortTable"; }

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
  ushort minUserKeyLen = 0; // now just used for runtime verify
  bool forceNeedCompact = true;
  bool collectProperties = true;
  bool accurateKeyAnchorsSize = false; // default disable
  uint32_t keyAnchorSizeUnit = 0;
  int debugLevel = 0;

  // stats
  struct FixedLenStats : gold_hash_map<int, int> {
    std::mutex mtx;
  };
  mutable FixedLenStats fixed_key_stats;
  mutable FixedLenStats fixed_value_stats;
  mutable long long start_time_point_ = 0;
  mutable long long build_time_duration_ = 0; // exclude idle time
  mutable size_t num_writers_ = 0;
  mutable size_t num_readers_ = 0;
  mutable size_t sum_user_key_len_ = 0;
  mutable size_t sum_user_key_cnt_ = 0;
  mutable size_t sum_value_len_ = 0;
  mutable size_t sum_index_len_ = 0;

  struct MetaInfo {
    uint32_t version;
    uint32_t padding; // make sure sizeof(MetaInfo) % 8 == 0
    uint32_t var_key_offsets_len;
    uint32_t min_ukey_len;
    uint32_t max_ukey_len;
    uint32_t pref_len;
    byte_t common_prefix[0];
    fstring prefix() const { return {common_prefix, pref_len}; }
    static MetaInfo* New(fstring pref) {
      auto p = (MetaInfo*)malloc(sizeof(MetaInfo) + pref.size());
      p->version = 1;
      p->padding = 0;
      p->var_key_offsets_len = 0;
      p->pref_len = pref.size();
      memcpy(p->common_prefix, pref.data(), pref.size());
      return p;
    }
    Slice Memory() const {
      return {(const char*)this, sizeof(MetaInfo) + pref_len};
    }
  };
};
using MetaInfo = VecAutoSortTableFactory::MetaInfo;
using fast_strvec = terark::fstrvec;
static_assert(sizeof(MetaInfo) == 24);

class VecAutoSortTableBuilder : public TopTableBuilderBase {
public:
  VecAutoSortTableBuilder(const VecAutoSortTableFactory*, const TableBuilderOptions&,
                       WritableFileWriter* file);
  ~VecAutoSortTableBuilder() {}
  void Add(const Slice& key, const Slice& value) final;
  Status GetBoundaryUserKey(std::string*, std::string*) const final;
  Status Finish() final;
  void Abandon() final;
  uint64_t EstimatedFileSize() const final {
    return strvec_.size() + lenbuf_.size();
  }
  void DoWriteAppend(const void* data, size_t size) {
    fobuf_.ensureWrite(data, size);
    offset_ += size;
  }
  void ToplingFlushBuffer();
  bool NeedCompact() const final;

  const VecAutoSortTableFactory* table_factory_;
  valvec<byte_t> strvec_;
  AutoGrownMemIO lenbuf_;
  ushort minUserKeyLen_;
  uint32_t min_key_len_ = UINT32_MAX, max_key_len_ = 0;
  uint32_t min_val_len_ = UINT32_MAX, max_val_len_ = 0;
  size_t num_asc_ = 0, num_dsc_ = 0;
  valvec<byte_t> min_ukey_, max_ukey_;
  OsFileStream fstream_;
  OutputBuffer fobuf_;
  long long t0 = 0;
  std::vector<std::pair<std::string, std::string> > kv_debug_;

  inline void write_varlen(size_t len) {
    if (LIKELY(len < 128))
      lenbuf_.writeByte(byte_t(len));
    else
      lenbuf_.write_var_uint32(uint32_t(len));
  }
  inline static uint32_t read_varlen(const byte_t*& ptr) {
    if (LIKELY(*ptr < 128))
      return *ptr++;
    else
      return terark::load_var_uint32(ptr, &ptr);
  }

  void remove_common_prefix() {
    const size_t num = num_user_key_;
    const byte_t* len_reader = lenbuf_.begin();
    byte_t* p_src = strvec_.data();
    byte_t* p_dst = strvec_.data();
    size_t  pref_len = commonPrefixLen(min_ukey_, max_ukey_);
    for (size_t i = 0; i < num; i++) {
      size_t key_len = read_varlen(len_reader);
      size_t val_len = read_varlen(len_reader);
      size_t kv_len = key_len + val_len;
      memmove(p_dst, p_src + pref_len, kv_len - pref_len);
      p_src += kv_len;
      p_dst += kv_len - pref_len;
    }
    ROCKSDB_VERIFY_EQ(size_t(p_src - strvec_.data()), strvec_.size());
    ROCKSDB_VERIFY_EQ(size_t(p_dst - strvec_.data()), strvec_.size() - pref_len * num);
    strvec_.resize(p_dst - strvec_.data());
    strvec_.shrink_to_fit();
  }

  /// fkvv_: fixed key var value
  ///@returns values
  ///@note: after this function returns, strvec_ contains only keys
  fast_strvec fkvv_split_kv(const size_t ukeyfixlen) {
    const uint32_t pref_len = commonPrefixLen(min_ukey_, max_ukey_);
    const size_t fixlen = ukeyfixlen + sizeof(uint32_t); // with value offset
    const size_t num = num_user_key_;
    fast_strvec values;
    TERARK_VERIFY_EQ(strvec_.size() - ukeyfixlen * num, properties_.raw_value_size);
    values.strpool.reserve(strvec_.size() - ukeyfixlen * num);
    values.offsets.reserve(num + 1);
    valvec<byte_t> keys(fixlen * (num+1), valvec_no_init());
    const byte_t* len_reader = lenbuf_.begin();
    byte_t* p_kv = strvec_.data();
    byte_t* p_key = keys.data();
    for (size_t i = 0; i < num; i++) {
      size_t key_len = read_varlen(len_reader);
      size_t val_len = read_varlen(len_reader);
      TERARK_VERIFY_EQ(key_len, pref_len + ukeyfixlen);
      values.strpool.unchecked_append(p_kv + ukeyfixlen, val_len);
      values.offsets.unchecked_push_back(values.strpool.size());
      memcpy(p_key, p_kv, ukeyfixlen);
      unaligned_save(p_key += ukeyfixlen, uint32_t(i)); // index
      p_key += sizeof(uint32_t);
      p_kv += ukeyfixlen + val_len;
    }
    TERARK_VERIFY_EQ(values.strpool.size(), properties_.raw_value_size);
    TERARK_VERIFY_EQ(size_t(p_key - keys.data()), fixlen * num);
    memset(p_key, 0, fixlen); // zero guard {content, index or offset}
    strvec_.swap(keys); // now strvec_ contains only keys
    lenbuf_.clear();
    return values;
  }

  SortThinStrVec vkfv_make_sort_strvec(size_t valfixlen) {
    const size_t num = num_user_key_;
    SortThinStrVec sort_strvec;
    sort_strvec.m_strpool.swap(strvec_);
    sort_strvec.m_index.resize_no_init(num);
    const byte_t* len_reader = lenbuf_.begin();
    uint32_t pref_len = commonPrefixLen(min_ukey_, max_ukey_);
    uint32_t offset = 0;
    for (size_t i = 0; i < num; i++) {
      uint32_t key_len = read_varlen(len_reader) - pref_len;
      uint32_t val_len = read_varlen(len_reader);
      ROCKSDB_ASSERT_EQ(val_len, valfixlen);
      sort_strvec.m_index[i] = {offset, key_len + val_len};
      offset += key_len + val_len;
    }
    sort_strvec.sort(valfixlen);
    lenbuf_.clear();
    return sort_strvec;
  }

  struct VKVV_Item {
    uint32_t offset;
    uint32_t key_len;
    uint32_t val_len;
  };
  struct CmpVKVV {
    bool operator()(const VKVV_Item& x, const VKVV_Item& y) const noexcept {
      const byte_t *xbeg = pool + x.offset;
      const byte_t *ybeg = pool + y.offset;
      return fstring(xbeg, x.key_len) < fstring(ybeg, y.key_len);
    }
    const byte_t* pool;
  };
  valvec<VKVV_Item> vkvv_make_sort_strvec() {
    const size_t num = num_user_key_;
    valvec<VKVV_Item> index(num, valvec_no_init());
    const byte_t* len_reader =lenbuf_.begin();
    uint32_t pref_len = commonPrefixLen(min_ukey_, max_ukey_);
    uint32_t offset = 0;
    for (size_t i = 0; i < num; i++) {
      uint32_t key_len = read_varlen(len_reader) - pref_len;
      uint32_t val_len = read_varlen(len_reader);
      index[i] = {offset, key_len, val_len};
      offset += key_len + val_len;
    }
    sort_a(index, CmpVKVV{strvec_.data()});
    lenbuf_.clear();
    return index;
  }

  void DebugCheckTable();
};

VecAutoSortTableBuilder::VecAutoSortTableBuilder(
        const VecAutoSortTableFactory* table_factory,
        const TableBuilderOptions& tbo,
        WritableFileWriter* file)
  : TopTableBuilderBase(tbo, file)
  , table_factory_(table_factory)
{
  properties_.compression_name = "VecAutoSort";
  debugLevel_ = table_factory->debugLevel;
  minUserKeyLen_ = table_factory->minUserKeyLen;
  int fd = (int)file->writable_file()->FileDescriptor();
  TERARK_VERIFY_GE(fd, 0);
  fstream_.attach(fd);
  fobuf_.attach(&fstream_);
  fobuf_.initbuf(table_factory->fileWriteBufferSize);
  lenbuf_.resize(64*1024);
  strvec_.reserve(256*1024);

  t0 = g_pf.now();
}

void VecAutoSortTableBuilder::Add(const Slice& key, const Slice& value) {
  properties_.num_entries++;
  const uint64_t seqvt = DecodeFixed64(key.data() + key.size() - 8);
  const auto vt = ValueType(seqvt & 255u);
  if (LIKELY(kTypeValue == vt)) {
    TERARK_ASSERT_GE(key.size(), 8);
    minimize(min_key_len_, key.size_); minimize(min_val_len_, value.size_);
    maximize(max_key_len_, key.size_); maximize(max_val_len_, value.size_);
    fstring ukey(key.data_, key.size_ - 8);
    TERARK_VERIFY_GE(ukey.size(), minUserKeyLen_);
    if (LIKELY(num_user_key_)) {
      if (false) {}
      else if (ukey < min_ukey_) min_ukey_.assign(ukey), num_dsc_++;
      else if (ukey > max_ukey_) max_ukey_.assign(ukey), num_asc_++;
    } else {
      min_ukey_.assign(ukey);
      max_ukey_.assign(ukey);
    }
    strvec_.append(key.data_, key.size_ - 8); // user key
    strvec_.append(value.data_, value.size_);
    write_varlen(ukey.size());
    write_varlen(value.size_);
    num_user_key_++;
    properties_.raw_key_size += key.size_;
    properties_.raw_value_size += value.size_;
    if (UNLIKELY(debugLevel_ >= 2)) {
      kv_debug_.emplace_back(ukey.str(), value.ToString());
    }
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

Status
VecAutoSortTableBuilder::GetBoundaryUserKey(std::string* smallest,
                                            std::string* largest) const {
  if (isReverseBytewiseOrder_) {
    std::swap(smallest, largest);
  }
  smallest->assign((char*)min_ukey_.data(), min_ukey_.size());
  largest->assign((char*)max_ukey_.data(), max_ukey_.size());
  return Status::OK();
}

struct KeyValueOffset {
  uint32_t key;
  uint32_t val;
};

template<class StrVec>
static auto getkey_of(const StrVec& strvec, size_t suffix_len) {
  return [&,suffix_len](size_t idx) { return strvec[idx].notail(suffix_len); };
}
static auto fkvv_get_val
(const FixedLenStrVec& fstrvec, const fast_strvec& values) {
  const size_t  fullfixlen = fstrvec.m_fixlen;
  const size_t  ukeyfixlen = fstrvec.m_fixlen - sizeof(uint32_t);
  const byte_t* val_idx_ptr = fstrvec.m_strpool.data() + ukeyfixlen;
  return [=,&values](size_t idx) {
    auto sorted_idx = *(const uint32_t*)(val_idx_ptr + fullfixlen * idx);
    return SliceOf(values[sorted_idx]);
  };
}

// fkvv: fixed key var value
static void fkvv_fill_sorted_offsets
(FixedLenStrVec& fstrvec, const valvec<uint32_t>& origin_offsets) {
  const size_t ukeyfixlen = fstrvec.m_fixlen - sizeof(uint32_t);
  const size_t num = fstrvec.size();
  ROCKSDB_VERIFY_GE(fstrvec.m_strpool.capacity(), fstrvec.m_fixlen * num);
  valvec<uint32_t> sorted_offsets(num + 1, valvec_no_init());
  byte_t* val_idx_ptr = fstrvec.m_strpool.data() + ukeyfixlen;
  uint32_t sorted_pos = 0;
  for (size_t i = 0; i < num; i++) {
    sorted_offsets[i] = sorted_pos;
    uint32_t  val_idx = *(const uint32_t*)(val_idx_ptr);
    uint32_t  pos_beg = origin_offsets[val_idx + 0];
    uint32_t  pos_end = origin_offsets[val_idx + 1];
    uint32_t  val_len = pos_end - pos_beg;
    sorted_pos += val_len;
    val_idx_ptr += fstrvec.m_fixlen;
  }
  sorted_offsets[num] = sorted_pos;
  // replace value index as sorted value offset
  val_idx_ptr = fstrvec.m_strpool.data() + ukeyfixlen;
  for (size_t i = 0; i < num + 1; i++) {
    *(uint32_t*)(val_idx_ptr) = sorted_offsets[i];
    val_idx_ptr += fstrvec.m_fixlen;
  }
  // now sorted offsets is stored after user_key in fixed strvec
  fstrvec.m_size = num + 1;
  fstrvec.m_strpool.risk_set_size(fstrvec.m_fixlen * (num + 1));
}

#undef DBG
#define DBG(fmt, ...) \
  do { \
    if (debugLevel_ >= 2) \
      fprintf(stderr, "%s:%d: %s: " fmt "\n", \
              __FILE__, __LINE__, __func__, ##__VA_ARGS__); \
  } while (0)

Status VecAutoSortTableBuilder::Finish() try {
  if (0 == num_user_key_) {
    FinishAsEmptyTable();
    return Status::OK();
  }
  if (debugLevel_ >= 2) {
    sort_a(kv_debug_);
  }
  properties_.num_data_blocks = 1;
  properties_.data_size = properties_.raw_value_size;
  TERARK_VERIFY_EQ(offset_, 0);
  using namespace std::placeholders;
  // NOLINTNEXTLINE
  auto writeAppend = std::bind(&VecAutoSortTableBuilder::DoWriteAppend, this, _1, _2);

  const uint32_t pref_len = commonPrefixLen(min_ukey_, max_ukey_);
  if (pref_len) {
    remove_common_prefix();
  }
  MetaInfo& sstmeta = *MetaInfo::New({min_ukey_.data(), pref_len});
  TERARK_SCOPE_EXIT(free(&sstmeta));
  sstmeta.min_ukey_len = min_key_len_ - 8;
  sstmeta.max_ukey_len = max_key_len_ - 8;

  if (min_key_len_ == max_key_len_) {
    table_factory_->fixed_key_stats.mtx.lock();
    table_factory_->fixed_key_stats[min_key_len_ - pref_len]++;
    table_factory_->fixed_key_stats.mtx.unlock();
  }
  if (min_val_len_ == max_val_len_) {
    table_factory_->fixed_value_stats.mtx.lock();
    table_factory_->fixed_value_stats[min_val_len_]++;
    table_factory_->fixed_value_stats.mtx.unlock();
  }

  const size_t num = num_user_key_;
  auto log = ioptions_.info_log.get();
  auto call_collector = [&](auto getkey, auto getval, size_t avg_len) {
    if (!table_factory_->collectProperties) {
      return;
    }
    valvec<byte_t> uk(max_key_len_, valvec_reserve());
    uk.assign(min_ukey_.data(), pref_len);
    for (size_t idx = 0; idx < num; idx++) {
      uk.risk_set_size(pref_len);
      uk.append(getkey(idx));
      TERARK_ASSERT_LE(uk.size(), sstmeta.max_ukey_len);
      unaligned_save<uint64_t>(uk.end(), PackSequenceAndType(0, kTypeValue));
      const Slice ikey((const char*)uk.data(), uk.size() + 8);
      uint64_t estimate_offset = avg_len * idx;
      Slice val = getval(idx);
      NotifyCollectTableCollectorsOnAdd(ikey, val, estimate_offset, collectors_, log);
    }
  };

  //---- Write StrVec Index
  if (min_key_len_ == max_key_len_) {
    const size_t ukeyfixlen = min_key_len_ - 8 - pref_len;
    properties_.fixed_key_len = min_key_len_; // internal key len
    terark::FixedLenStrVec fstrvec;
    fstrvec.m_size = num;
    sstmeta.pref_len = pref_len;
    if (min_val_len_ == max_val_len_) {
      DBG("fkfv: pref_len = %d, fixsuf = %zd, fixval = %d", pref_len, ukeyfixlen, min_val_len_);
      properties_.fixed_value_len = min_val_len_;
      lenbuf_.clear(); // free memory
      const size_t valfixlen = min_val_len_;
      const size_t kvfixlen = ukeyfixlen + valfixlen;
      TERARK_VERIFY_EQ(strvec_.size(), kvfixlen * num);
      fstrvec.m_fixlen = kvfixlen;
      fstrvec.m_strpool.swap(strvec_);
      if (num_asc_ + 1 == num) { // input is foward bytewise sorted
        fstrvec.sort(valfixlen); // TODO: omit sort and test
      } else if (num_dsc_ + 1 == num) { // input is reverse bytewise sorted
        fstrvec.sort(valfixlen); // TODO: just reverse
      } else { // need sort
        fstrvec.sort(valfixlen);
      }
      auto base_valptr = (const char*)fstrvec.m_strpool.data() + ukeyfixlen;
      auto getval = [=](size_t idx) {
        ROCKSDB_ASSERT_LT(idx, num);
        return Slice{base_valptr + kvfixlen*idx, valfixlen};
      };
      call_collector(getkey_of(fstrvec, valfixlen), getval, kvfixlen);
      DoWriteAppend(fstrvec.m_strpool.data(), fstrvec.m_strpool.size());
    } else {
      DBG("fkvv: pref_len = %d, fixsuf = %zd", pref_len, ukeyfixlen);
      auto values = fkvv_split_kv(ukeyfixlen);
      fstrvec.m_fixlen = ukeyfixlen + sizeof(uint32_t);
      fstrvec.m_strpool.swap(strvec_);
      fstrvec.m_strpool.risk_set_size(fstrvec.m_fixlen * num);
      fstrvec.sort(sizeof(uint32_t));
      auto getval = fkvv_get_val(fstrvec, values);
      auto avg_kvlen = fstrvec.m_fixlen + values.strpool.size() / num;
      call_collector(getkey_of(fstrvec, sizeof(uint32_t)), getval, avg_kvlen);
      for (size_t i = 0; i < num; i++) { // write values contents
        fstring content = getval(i);
        fobuf_.ensureWrite(content.p, content.n);
      }
      offset_ += values.strpool.size(); // update offset_ for values
      Padzero<64>(writeAppend, offset_);
      values.strpool.clear(); // now values.strpool is useless, free memory
      fkvv_fill_sorted_offsets(fstrvec, values.offsets);
      values.offsets.clear(); // now values.offsets is useless, free memory
      // fstrvec.m_strpool has guard elem, verify for future broken changes
      TERARK_VERIFY_EQ(fstrvec.m_strpool.size(), fstrvec.m_fixlen*(num+1));
      DoWriteAppend(fstrvec.m_strpool.data(), fstrvec.m_strpool.size());
    }
    properties_.index_size = fstrvec.m_strpool.size();
  }
  else { // var len key
    if (min_val_len_ == max_val_len_) { // fixed len value
      DBG("vkfv: pref_len = %d, fixval = %d", pref_len, min_val_len_);
      properties_.fixed_value_len = min_val_len_;
      const size_t valfixlen = min_val_len_;
      SortThinStrVec sort_strvec = vkfv_make_sort_strvec(valfixlen);
      auto getval = [&](size_t i) {
        fstring kv = sort_strvec[i];
        return Slice{kv.end() - valfixlen, valfixlen};
      };
      size_t sum_keylen = sort_strvec.m_strpool.size() - valfixlen * num;
      UintVecMin0 offsets(num + 1, sum_keylen);
      auto wire_size = sort_strvec.str_size() + offsets.mem_size();
      auto avg_kvlen = wire_size / num + valfixlen;
      call_collector(getkey_of(sort_strvec, valfixlen), getval, avg_kvlen);
      for (size_t i = 0; i < num; i++) { // write values
        fstring kv = sort_strvec[i];
        ROCKSDB_ASSERT_LE(min_key_len_ - 8 + valfixlen, pref_len + kv.size());
        fobuf_.ensureWrite(kv.end() - valfixlen, valfixlen);
      }
      offset_ += valfixlen * num;
      Padzero<64>(writeAppend, offset_);
      for (size_t i = 0; i < num; i++) { // write key contents
        fstring kv = sort_strvec[i];
        DoWriteAppend(kv.data(), kv.size() - valfixlen);
      }
      Padzero<64>(writeAppend, offset_);
      size_t offset = 0;
      for (size_t i = 0; i < num; i++) { // calculate key offsets
        size_t kvlen = sort_strvec.nth_size(i);
        offsets.set_wire(i, offset);
        offset += kvlen - valfixlen;
      }
      offsets.set_wire(num, offset);
      ROCKSDB_VERIFY_EQ(offset, sum_keylen);
      DoWriteAppend(offsets.data(), offsets.mem_size());
      sstmeta.var_key_offsets_len = offsets.mem_size();
      properties_.index_size = offsets.mem_size() + sum_keylen;
    }
    else { // var len value
      DBG("vkvv: pref_len = %d", pref_len);
      auto index = vkvv_make_sort_strvec();
      auto getkey = [&](size_t idx) {
        const VKVV_Item& e = index[idx];
        return fstring(strvec_.ptr(e.offset), e.key_len);
      };
      auto getval = [&](size_t idx) {
        const VKVV_Item& e = index[idx];
        return Slice((char*)strvec_.ptr(e.offset) + e.key_len, e.val_len);
      };
      auto avg_kvlen = strvec_.size() / num + 8;
      call_collector(getkey, getval, avg_kvlen);
      for (size_t i = 0; i < num; ++i) { // write values
        auto val = getval(i);
        DoWriteAppend(val.data(), val.size());
      }
      Padzero<64>(writeAppend, offset_);
      for (size_t i = 0; i < num; ++i) { // write keys
        auto key = getkey(i);
        DoWriteAppend(key.data(), key.size());
      }
      Padzero<64>(writeAppend, offset_);
      KeyValueOffset kvoffset{0, 0};
      for (size_t i = 0; i < num; ++i) { // write index: KeyValueOffset
        const VKVV_Item& e = index[i];
        fobuf_.ensureWrite(&kvoffset, sizeof(kvoffset));
        kvoffset.key += e.key_len;
        kvoffset.val += e.val_len;
      }
      fobuf_.ensureWrite(&kvoffset, sizeof(kvoffset)); // guard
      offset_ += sizeof(kvoffset) * (num + 1);
      properties_.index_size = sizeof(kvoffset) * (num + 1) + kvoffset.key;
    }
  }
  //---- End Write StrVec Index

  ToplingFlushBuffer();
  WriteMeta(kVecAutoSortTableMagic,
    {{kStrVecIndex, WriteBlock(sstmeta.Memory(), file_, &offset_)}});

  auto fac = table_factory_;
  auto ukey_len = properties_.raw_key_size - 8 * properties_.num_entries;
  long long t1 = g_pf.now();
  auto td = g_pf.us(t1 - t0);
  as_atomic(fac->sum_user_key_cnt_).fetch_add(num_user_key_, std::memory_order_relaxed);
  as_atomic(fac->sum_user_key_len_).fetch_add(ukey_len, std::memory_order_relaxed);
  as_atomic(fac->sum_value_len_).fetch_add(properties_.raw_value_size, std::memory_order_relaxed);
  as_atomic(fac->sum_index_len_).fetch_add(properties_.index_size, std::memory_order_relaxed);
  as_atomic(fac->build_time_duration_).fetch_add(td, std::memory_order_relaxed); // in us

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

std::unique_ptr<TableReader>
OpenSST(const std::string& fname, uint64_t fsize, const ImmutableOptions&);
static std::string hex(Slice s) { return s.ToString(true); }
void VecAutoSortTableBuilder::DebugCheckTable() {
  auto tr_uptr = OpenSST(file_->file_name(), offset_, ioptions_);
  auto tr = tr_uptr.get();
  auto new_iter = [&]() {
    ReadOptions ro;
    return tr->NewIterator(ro, nullptr, nullptr, false, kUserIterator);
  };
  auto it = new_iter();
  it->SeekToFirst();
  TERARK_VERIFY_S(it->Valid(), "status = %s", it->status().ToString());
  size_t idx = 0, num = kv_debug_.size();
  while (it->Valid()) {
    TERARK_VERIFY_LT(idx, num);
    TERARK_VERIFY_S(kv_debug_[idx].first == it->user_key(), " %s <=> %s",
                hex(kv_debug_[idx].first), hex(it->user_key()));
    TERARK_VERIFY_S(kv_debug_[idx].second == it->value(), " %s <=> %s",
                hex(kv_debug_[idx].second), hex(it->value()));
    it->Next();
    idx++;
  }
  TERARK_VERIFY_EQ(idx, num);
  it->SeekToLast();
  TERARK_VERIFY_S(it->Valid(), "status = %s", it->status().ToString());
  idx = num;
  while (it->Valid()) { // reverse iterate check
    idx--;
    TERARK_VERIFY_LT(idx, num);
    TERARK_VERIFY_S(kv_debug_[idx].first == it->user_key(), " %s <=> %s",
                hex(kv_debug_[idx].first), hex(it->user_key()));
    TERARK_VERIFY_S(kv_debug_[idx].second == it->value(), " %s <=> %s",
                hex(kv_debug_[idx].second), hex(it->value()));
    it->Prev();
  }
  for (idx = 0; idx < num; idx++) { // Seek check
    Slice uk = kv_debug_[idx].first;
    it->Seek(uk + Slice("\0\0\0\0\0\0\0\0", 8));
    TERARK_VERIFY_S(it->Valid(), "%zd/%zd : %s", idx, num, hex(uk));
    TERARK_VERIFY_S(kv_debug_[idx].first == it->user_key(), " %s <=> %s",
                hex(kv_debug_[idx].first), hex(it->user_key()));
    TERARK_VERIFY_S(kv_debug_[idx].second == it->value(), " %s <=> %s",
                hex(kv_debug_[idx].second), hex(it->value()));
  }
  STD_INFO("VecAutoSortTableBuilder::DebugCheckTable(%s) success!",
           file_->file_name().c_str());

  delete it;
}

void VecAutoSortTableBuilder::Abandon() {
  fobuf_.resetbuf(); // discard buffer content
  ToplingFlushBuffer();
  closed_ = true;
}

void VecAutoSortTableBuilder::ToplingFlushBuffer() {
  fobuf_.flush_buffer();
  fstream_.detach();
  file_->SetFileSize(offset_); // fool the WritableFileWriter
  file_->writable_file()->SetFileSize(offset_); // fool the WritableFile
  IOSTATS_ADD(bytes_written, offset_);
}

bool VecAutoSortTableBuilder::NeedCompact() const {
  if (table_factory_->forceNeedCompact) {
    return true;
  }
  return TopTableBuilderBase::NeedCompact();
}

TableBuilder*
VecAutoSortTableFactory::NewTableBuilder(const TableBuilderOptions& tbo,
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
  return new VecAutoSortTableBuilder(this, tbo, file);
}

class VecAutoSortTableReader : public TopTableReaderBase {
public:
  VecAutoSortTableReader(const VecAutoSortTableFactory* f) : factory_(f) {}
  ~VecAutoSortTableReader() override;
  void Open(RandomAccessFileReader*, Slice file_data, const TableReaderOptions&);
  InternalIterator*
  NewIterator(const ReadOptions&, const SliceTransform* prefix_extractor,
              Arena* arena, bool skip_filters, TableReaderCaller caller,
              size_t compaction_readahead_size,
              bool allow_unprepared_value) final;

  uint64_t ApproximateOffsetOf(ROCKSDB_8_X_COMMA(const ReadOptions& readopt) const Slice& key, TableReaderCaller) final;
  uint64_t ApproximateSize(ROCKSDB_8_X_COMMA(const ReadOptions& readopt) const Slice&, const Slice&, TableReaderCaller) final;
  size_t ApproximateMemoryUsage() const final { return file_data_.size_; }
  Status Get(const ReadOptions& readOptions, const Slice& key,
                     GetContext* get_context,
                     const SliceTransform* prefix_extractor,
                     bool skip_filters) final;
  template<class StrVec>
  Status GetImpl(const ReadOptions&, const Slice& ikey, GetContext*, const StrVec&);

  Status VerifyChecksum(const ReadOptions&, TableReaderCaller) final;
  bool GetRandomInternalKeysAppend(size_t num, std::vector<std::string>* output) const final;
#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 70060
  Status ApproximateKeyAnchors(const ReadOptions&, std::vector<Anchor>&) override;
#endif
  std::string ToWebViewString(const json& dump_options) const final;

// data member also public
  struct vkvv_t {
    valvec<KeyValueOffset> kvoffsets;
    const byte_t* key_base;
    const byte_t* val_base;
    fstring key(size_t idx) const {
      uint32_t beg = kvoffsets[idx + 0].key;
      uint32_t end = kvoffsets[idx + 1].key;
      return {key_base + beg, end - beg};
    }
    fstring val(size_t idx) const {
      uint32_t beg = kvoffsets[idx + 0].val;
      uint32_t end = kvoffsets[idx + 1].val;
      return {val_base + beg, end - beg};
    }
    fstring operator[](size_t idx) const { return key(idx); }
    size_t lower_bound(fstring searchkey) const {
      size_t lo = 0, hi = kvoffsets.size() - 1;
      while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        fstring mkey = key(mid);
        if (mkey < searchkey)
          lo = mid + 1;
        else
          hi = mid;
      }
      return lo;
    }
    size_t upper_bound(fstring searchkey) const {
      size_t lo = 0, hi = kvoffsets.size() - 1;
      while (lo < hi) {
        size_t mid = (lo + hi) / 2;
        fstring mkey = key(mid);
        if (mkey <= searchkey)
          lo = mid + 1;
        else
          hi = mid;
      }
      return lo;
    }
    size_t size() const { return kvoffsets.size() - 1; }
  };

  fstring GetKey(size_t idx) const {
    if (fixed_key_len_)
      if (fixed_value_len_ >= 0)
        return fstrvec_[idx].notail(fixed_value_len_);
      else
        return fstrvec_[idx].notail(sizeof(uint32_t));
    else
      if (fixed_value_len_ >= 0)
        return ssvec_[idx];
      else
        return vkvv_[idx];
  }
  size_t NumUserKeys() const {
    if (fixed_key_len_) {
      return fstrvec_.size();
    } else if (fixed_value_len_ >= 0) {
      return ssvec_.size();
    } else {
      return vkvv_.size();
    }
  }
  union {
    FixedLenStrVec fstrvec_; // fixed key any value
    SortedStrVec   ssvec_;   // vkfv: var key fixed value
    vkvv_t vkvv_; // vkvv: var key var value
  };
  int fixed_key_len_ = 0; // 0 means var len
  int fixed_value_len_ = -1; // -1 means var len
  uint32_t pref_len_;
  const MetaInfo* sstmeta_;
  const VecAutoSortTableFactory* factory_;

  class Iter;
  class BaseIter;
  class RevIter;
};

uint64_t
VecAutoSortTableReader::ApproximateOffsetOf(ROCKSDB_8_X_COMMA(const ReadOptions& readopt)
                        const Slice& ikey, TableReaderCaller) {
  TERARK_VERIFY_GE(ikey.size(), 8);
  fstring user_key(ikey.data(), ikey.size() - 8);
  int cmp = memcmp(sstmeta_->common_prefix, user_key.data(),
                   std::min<size_t>(pref_len_, user_key.size()));
  if (isReverseBytewiseOrder_) {
    if (cmp > 0) return file_data_.size_;
    if (cmp < 0) return 0;
    if (user_key.size() < pref_len_) {
      return file_data_.size_;
    }
    user_key = user_key.substr(pref_len_);
    if (fixed_key_len_) {
      size_t lo = fstrvec_.size() - fstrvec_.lower_bound(user_key);
      return file_data_.size_ * lo / fstrvec_.size();
    } else {
      size_t lo = ssvec_.size() - ssvec_.lower_bound(user_key);
      return file_data_.size_ * lo / ssvec_.size();
    }
  }
  else {
    if (cmp > 0) return 0;
    if (cmp < 0) return file_data_.size_;
    if (user_key.size() < pref_len_) {
      return 0;
    }
    user_key = user_key.substr(pref_len_);
    if (fixed_key_len_) {
      size_t lo = fstrvec_.lower_bound(user_key);
      return file_data_.size_ * lo / fstrvec_.size();
    } else {
      size_t lo = ssvec_.lower_bound(user_key);
      return file_data_.size_ * lo / ssvec_.size();
    }
  }
  return file_data_.size_;
}
uint64_t VecAutoSortTableReader::ApproximateSize(ROCKSDB_8_X_COMMA(const ReadOptions& readopt)
                                              const Slice& beg, const Slice& end,
                                              TableReaderCaller caller) {
  uint64_t offset_beg = ApproximateOffsetOf(ROCKSDB_8_X_COMMA(readopt)beg, caller);
  uint64_t offset_end = ApproximateOffsetOf(ROCKSDB_8_X_COMMA(readopt)end, caller);
  return offset_end - offset_beg;
}

Status VecAutoSortTableReader::Get(const ReadOptions& ro,
                                const Slice& ikey,
                                GetContext* get_context,
                                const SliceTransform* /*prefix_extractor*/,
                                bool /*skip_filters*/) {
  ParsedInternalKey pikey(ikey);
  Status st;
  if (global_seqno_ <= pikey.sequence) {
    int cmp = memcmp(sstmeta_->common_prefix, pikey.user_key.data(),
                     std::min<size_t>(pref_len_, pikey.user_key.size()));
    if (cmp != 0 || pikey.user_key.size() < pref_len_) return st;
    fstring suffix = fstring(pikey.user_key).substr(pref_len_);
    Slice val;
    Cleanable noop_pinner;
    Cleanable* pinner = ro.pinning_tls ? &noop_pinner : nullptr;
    if (fixed_key_len_) {
      if (ikey.size_ != size_t(fixed_key_len_)) {
        return st;
      }
      size_t lo = fstrvec_.lower_bound_prefix(suffix);
      if (lo < fstrvec_.size() &&
memcmp(fstrvec_.nth_data(lo), suffix.data(), suffix.size()) == 0) {
        if (!ro.just_check_key_exists) {
          if (fixed_value_len_ >= 0) {
            val.data_ = file_data_.data_ + fixed_value_len_ * lo;
            val.size_ = fixed_value_len_;
          }
          else {
            size_t offset = unaligned_load<uint32_t>(fstrvec_[lo].end() - 4);
            val.data_ = file_data_.data_ + offset;
            val.size_ = fixed_value_len_;
          }
        }
        get_context->SaveValue(pikey, val, pinner);
      }
    }
    else { // 0 == fixed_key_len_
      if (fixed_value_len_ >= 0) {
        size_t lo = ssvec_.lower_bound(suffix);
        if (lo < ssvec_.size() && ssvec_[lo] == suffix) {
          if (!ro.just_check_key_exists) {
            val.data_ = file_data_.data_ + fixed_value_len_ * lo;
            val.size_ = fixed_value_len_;
          }
          get_context->SaveValue(pikey, val, pinner);
        }
      } else { // fixed_value_len_ < 0
        size_t lo = vkvv_.lower_bound(suffix);
        if (lo < vkvv_.size() && vkvv_[lo] == suffix) {
          if (!ro.just_check_key_exists) {
            val = SliceOf(vkvv_.val(lo));
          }
          get_context->SaveValue(pikey, val, pinner);
        }
      }
    }
  }
  return st;
}

Status VecAutoSortTableReader::VerifyChecksum(const ReadOptions&, TableReaderCaller) {
  return Status::OK();
}

class VecAutoSortTableReader::BaseIter : public InternalIterator, boost::noncopyable {
public:
  const VecAutoSortTableReader* tab_;
  const char* file_data_;
  uint64_t global_seqno_;
  char* ikey_buf_;
  int fixed_key_len_;
  int fixed_value_len_;
  uint32_t pref_len_;
  uint16_t minUserKeyLen_;
  bool isReverseBytewiseOrder_;
  int num_;
  int idx_;
  uint32_t ikey_len_;
  char  ikey_internal_buf_[0];
  explicit BaseIter(const VecAutoSortTableReader* table, bool is_arena) {
    tab_ = table;
    file_data_ = table->file_data_.data_;
    global_seqno_ = table->global_seqno_;
    fixed_key_len_ = table->fixed_key_len_;
    fixed_value_len_ = table->fixed_value_len_;
    pref_len_ = table->pref_len_;
    minUserKeyLen_ = table->factory_->minUserKeyLen;
    isReverseBytewiseOrder_ = table->isReverseBytewiseOrder_;
    num_ = table->table_properties_->num_entries;
    idx_ = -1;
    if (is_arena)
      ikey_buf_ = ikey_internal_buf_;
    else
      ikey_buf_ = (char*)malloc(table->sstmeta_->max_ukey_len + 8);
    memcpy(ikey_buf_, table->sstmeta_->common_prefix, pref_len_);
  }
  ~BaseIter() override {
    as_atomic(tab_->live_iter_num_).fetch_sub(1, std::memory_order_relaxed);
    if (ikey_buf_ != ikey_internal_buf_) free(ikey_buf_);
  }
  void SetSeqVT(bool is_valid) {
    if (is_valid) {
      auto suffix = tab_->GetKey(idx_);
      TERARK_VERIFY_GE(pref_len_ + suffix.size(), minUserKeyLen_);
      memcpy(ikey_buf_ + pref_len_, suffix.data(), suffix.size());
      ikey_len_ = pref_len_ + suffix.size() + 8;
      uint64_t seqvt = PackSequenceAndType(global_seqno_, kTypeValue);
      unaligned_save<uint64_t>(ikey_buf_ + ikey_len_ - 8, seqvt);
    }
  }
  void SetPinnedItersMgr(PinnedIteratorsManager*) final {}
  bool Valid() const final { return uint(idx_) < uint(num_); }
  void SeekForPrevAux(const Slice& target, const InternalKeyComparator& c) {
    SeekForPrevImpl(target, &c);
  }
  void SeekForPrev(const Slice& target) final {
    if (isReverseBytewiseOrder_)
      SeekForPrevAux(target, InternalKeyComparator(ReverseBytewiseComparator()));
    else
      SeekForPrevAux(target, InternalKeyComparator(BytewiseComparator()));
  }
  Slice key() const final {
    TERARK_ASSERT_BT(idx_, 0, num_);
    TERARK_ASSERT_GE(ikey_len_, minUserKeyLen_);
    return Slice(ikey_buf_, ikey_len_);
  }
  Slice user_key() const final {
    TERARK_ASSERT_BT(idx_, 0, num_);
    return Slice(ikey_buf_, ikey_len_ - 8);
  }
  Slice value() const final {
    TERARK_ASSERT_BT(idx_, 0, num_);
    intptr_t fixed_key_len = fixed_key_len_;
    intptr_t fixed_value_len = fixed_value_len_;
    if (fixed_key_len) {
      const size_t fixed_suffix_len = fixed_key_len - pref_len_ - 8;
      if (fixed_value_len >= 0) {
        const size_t fixlen = fixed_suffix_len + fixed_value_len;
        const char* p_value = file_data_ + fixlen * idx_ + fixed_suffix_len;
        return Slice(p_value, fixed_value_len);
      } else {
        const byte_t* index_base = tab_->fstrvec_.m_strpool.data();
        const size_t  fixlen = fixed_suffix_len + 4;
        auto beg = unaligned_load<uint32_t>(index_base + fixlen * (idx_ + 1) - 4);
        auto end = unaligned_load<uint32_t>(index_base + fixlen * (idx_ + 2) - 4);
        return {file_data_ + beg, end - beg};
      }
    } else { // 0 == fixed_key_len
      if (fixed_value_len >= 0) {
        return Slice(file_data_ + fixed_value_len * idx_, fixed_value_len);
      } else {
        return SliceOf(tab_->vkvv_.val(idx_));
      }
    }
  }
  Status status() const final { return Status::OK(); }
  bool IsKeyPinned() const final { return false; }
  bool IsValuePinned() const final { return true; }
};
class VecAutoSortTableReader::Iter : public BaseIter {
public:
  using BaseIter::BaseIter;
  void SeekToFirst() final {
    idx_ = 0;
    SetSeqVT(true);
  }
  void SeekToLast() final {
    idx_ = num_ - 1;
    SetSeqVT(true);
  }
  void Seek(const Slice& ikey) final {
    ROCKSDB_VERIFY_GE(ikey.size_, 8);
    size_t plen = pref_len_;
    fstring ukey(ikey.data_, ikey.size_-8);
    uint64_t seq;
    ValueType vt;
    UnPackSequenceAndType(unaligned_load<uint64_t>(ukey.end()), &seq, &vt);
    int cmp = memcmp(ikey_buf_, ukey.data(), std::min(plen, ukey.size()));
    if (cmp < 0) { // search key is larger than all keys in this SST
      idx_ = -1;
      return;
    }
    if (cmp > 0 || ukey.size() <= plen) {
      // search key is smaller than all keys in this SST
      SeekToFirst();
      return;
    }
    if (fixed_key_len_) {
      size_t fixed_ukey_len = fixed_key_len_ - 8;
      size_t suffix_len = std::min(ukey.size(), fixed_ukey_len) - plen;
      fstring suffix = ukey.substr(plen, suffix_len);
      idx_ = tab_->fstrvec_.lower_bound_prefix(suffix);
      if (idx_ < num_ && ukey.size() > fixed_ukey_len) {
        auto hit_key_data = tab_->fstrvec_.nth_data(idx_);
        if (memcmp(suffix.p, hit_key_data, suffix_len) == 0) {
          // prefix match but ukey.size is larger
          idx_++;
        }
      }
    } else {
      fstring suffix = ukey.substr(plen);
      if (fixed_value_len_ >= 0)
        idx_ = tab_->ssvec_.lower_bound(suffix);
      else
        idx_ = tab_->vkvv_.lower_bound(suffix);
    }
    if (global_seqno_ > seq) {
      idx_++;
    }
    SetSeqVT(idx_ < num_);
  }
  void Next() final {
    TERARK_ASSERT_BT(idx_, 0, num_);
    SetSeqVT(++idx_ < num_);
  }
  void Prev() final {
    TERARK_ASSERT_BT(idx_, 0, num_);
    SetSeqVT(--idx_ >= 0);
  }
  bool NextAndGetResult(IterateResult* result) final {
    TERARK_ASSERT_BT(idx_, 0, num_);
    Next();
    if (this->Valid()) {
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
class VecAutoSortTableReader::RevIter : public BaseIter {
public:
  using BaseIter::BaseIter;
  void SeekToFirst() final {
    idx_ = num_ - 1;
    SetSeqVT(true);
  }
  void SeekToLast() final {
    idx_ = 0;
    SetSeqVT(true);
  }
  void Seek(const Slice& ikey) final {
    ROCKSDB_VERIFY_GE(ikey.size_, 8);
    size_t plen = pref_len_;
    fstring ukey(ikey.data_, ikey.size_-8);
    uint64_t seq;
    ValueType vt;
    UnPackSequenceAndType(unaligned_load<uint64_t>(ukey.end()), &seq, &vt);
    int cmp = memcmp(ikey_buf_, ukey.data(), std::min(plen, ukey.size()));
    if (cmp > 0) { // search key is larger than all keys in this SST
      idx_ = -1;
      return;
    }
    if (cmp < 0) {
      // search key is smaller than all keys in this SST
      SeekToFirst();
      return;
    }
    if (UNLIKELY(ukey.size() <= plen)) {
      if (UNLIKELY(ukey.size() == plen))
        SeekToLast(); // min ukey suffix is empty
      else
        idx_ = -1;
      return;
    }
    if (fixed_key_len_) {
      size_t fixed_ukey_len = fixed_key_len_ - 8;
      size_t suffix_len = std::min(ukey.size(), fixed_ukey_len) - plen;
      fstring suffix = ukey.substr(plen, suffix_len);
      idx_ = tab_->fstrvec_.upper_bound_prefix(suffix) - 1;
    } else {
      fstring suffix = ukey.substr(plen);
      if (fixed_value_len_ >= 0)
        idx_ = tab_->ssvec_.upper_bound(suffix) - 1;
      else
        idx_ = tab_->vkvv_.upper_bound(suffix) - 1;
    }
    if (global_seqno_ > seq) {
      idx_--;
    }
    SetSeqVT(idx_ >= 0);
  }
  void Next() final {
    TERARK_ASSERT_BT(idx_, 0, num_);
    SetSeqVT(--idx_ >= 0);
  }
  void Prev() final {
    TERARK_ASSERT_BT(idx_, 0, num_);
    SetSeqVT(++idx_ < num_);
  }
  bool NextAndGetResult(IterateResult* result) final {
    TERARK_ASSERT_BT(idx_, 0, num_);
    Next();
    if (this->Valid()) {
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
InternalIterator*
VecAutoSortTableReader::NewIterator(const ReadOptions& ro,
                                   const SliceTransform* prefix_extractor,
                                   Arena* a,
                                   bool skip_filters,
                                   TableReaderCaller caller,
                                   size_t compaction_readahead_size,
                                   bool allow_unprepared_value)
{
  as_atomic(live_iter_num_).fetch_add(1, std::memory_order_relaxed);
  using RevI = RevIter;
  static_assert(sizeof(Iter) == sizeof(RevIter));
  size_t IterMemSize = sizeof(Iter) + sstmeta_->max_ukey_len + 8;
  if (isReverseBytewiseOrder_)
    return a ? new(a->AllocateAligned(IterMemSize))RevI(this, 1) : new RevI(this, 0);
  else
    return a ? new(a->AllocateAligned(IterMemSize))Iter(this, 1) : new Iter(this, 0);
}

bool VecAutoSortTableReader::GetRandomInternalKeysAppend(
                  size_t num, std::vector<std::string>* output) const {
  if (0 == num) {
    return true;
  }
  const size_t num_ukeys = NumUserKeys();
  num = std::min(num, num_ukeys);
  const auto step = double(num_ukeys) / num;
  for (size_t i = 0; i < num; ++i) {
    const size_t r = std::min(size_t(step * i), num_ukeys - 1);
    fstring ukey;
    if (fixed_key_len_) {
      if (fixed_value_len_ >= 0)
        ukey = fstrvec_[r].prefix(fixed_key_len_).str();
      else
        ukey = fstrvec_[r].notail(sizeof(uint32_t)).str();
    } else {
      if (fixed_value_len_ >= 0)
        ukey = ssvec_[r];
      else
        ukey = vkvv_.key(r);
    }
    uint64_t seqvt = PackSequenceAndType(global_seqno_, kTypeValue);
    std::string ikey;
    ikey.reserve(pref_len_ + ukey.size() + 8);
    ikey.append((char*)sstmeta_->common_prefix, pref_len_);
    ikey.append(ukey.data(), ukey.size());
    ikey.append((char*)&seqvt, sizeof(seqvt));
    output->push_back(std::move(ikey));
  }
  return true;
}

#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 70060
Status VecAutoSortTableReader::ApproximateKeyAnchors
            (const ReadOptions& ro, std::vector<Anchor>& anchors) {
  if (!factory_->accurateKeyAnchorsSize) {
    return TopTableReaderBase::ApproximateKeyAnchors(ro, anchors);
  }
  size_t an_num = 256;
  if (size_t keyAnchorSizeUnit = factory_->keyAnchorSizeUnit) {
    size_t units = file_data_.size_ / keyAnchorSizeUnit;
    an_num = std::min<size_t>(units, 10000);
    an_num = std::max<size_t>(an_num, 256);
  }
  const size_t num_ukeys = NumUserKeys();
  an_num = std::min(an_num, num_ukeys);
  anchors.reserve(an_num);
  const auto step = double(num_ukeys) / an_num;
  const auto fixed_key_len = fixed_key_len_;
  const auto fixed_value_len = fixed_value_len_;
  const auto pref_len = pref_len_;
  const byte_t* index_base = fstrvec_.m_strpool.data();
  const size_t fixed_suffix_len = fixed_key_len - pref_len - 8;
  const size_t fixlen = fixed_suffix_len + 4;
  size_t prev_key_offset = 0, prev_val_offset = 0;
  for (size_t i = 0; i < an_num; ++i) {
    const size_t r = std::min(size_t(step * i), num_ukeys - 1);
    fstring suffix;
    size_t curr_key_offset, curr_val_offset; // end offset
    if (fixed_key_len) {
      if (fixed_value_len >= 0) {
        suffix = fstrvec_[r].prefix(fixed_key_len).str();
        curr_key_offset = (r+1) * fixed_key_len;
        curr_val_offset = (r+1) * fixed_value_len;
      } else {
        suffix = fstrvec_[r].notail(sizeof(uint32_t)).str();
        curr_key_offset = (r+1) * fixed_key_len;
        curr_val_offset = unaligned_load<uint32_t>(index_base + fixlen * (r + 2) - 4);
      }
    } else {
      if (fixed_value_len >= 0) {
        suffix = ssvec_[r];
        curr_key_offset = ssvec_.m_offsets[r+1];
        curr_val_offset = (r+1) * fixed_value_len;
      } else {
        suffix = vkvv_.key(r);
        curr_key_offset = vkvv_.kvoffsets[r+1].key;
        curr_val_offset = vkvv_.kvoffsets[r+1].val;
      }
    }
    std::string ukey;
    ukey.reserve(pref_len + suffix.size());
    ukey.append((char*)sstmeta_->common_prefix, pref_len);
    ukey.append(suffix.data(), suffix.size());
    auto curr_kv = curr_key_offset + curr_val_offset;
    auto prev_kv = prev_key_offset + prev_val_offset;
    anchors.push_back({std::move(ukey), curr_kv - prev_kv});
  }
  return Status::OK();
}
#endif

std::string VecAutoSortTableReader::ToWebViewString(const json& dump_options) const {
  return "VecAutoSortTableReader::ToWebViewString: to be done";
}

/////////////////////////////////////////////////////////////////////////////

void VecAutoSortTableReader::Open(RandomAccessFileReader* file, Slice file_data, const TableReaderOptions& tro) {
  uint64_t file_size = file_data.size_;
  try {
    LoadCommonPart(file, tro, file_data, kVecAutoSortTableMagic);
  }
  catch (const Status&) { // very rare, try EmptyTable
    BlockContents emptyTableBC = ReadMetaBlockE(
        file, file_size, kTopEmptyTableMagicNumber,
        tro.ioptions, kTopEmptyTableKey);
    TERARK_VERIFY(!emptyTableBC.data.empty());
    INFO(tro.ioptions.info_log,
         "VecAutoSortTableReader::Open: %s is EmptyTable, it's ok\n",
         file->file_name().c_str());
    auto t = UniquePtrOf(new TopEmptyTableReader());
    file_.release(); // NOLINT
    t->Open(file, file_data, tro);
    throw t.release(); // NOLINT
  }
  BlockContents indexBlock = ReadMetaBlockE(file, file_size,
        kVecAutoSortTableMagic, tro.ioptions, kStrVecIndex);
  auto& sstmeta = *(const MetaInfo*)(indexBlock.data.data_);
  this->sstmeta_ = &sstmeta;
  this->pref_len_ = sstmeta.pref_len;
  byte_t* base = (byte_t*)file_data_.data_;
  fixed_key_len_ = int(table_properties_->fixed_key_len); // internal key
  fixed_value_len_ = int(table_properties_->fixed_value_len);
  TERARK_VERIFY_LE(pref_len_, sstmeta.min_ukey_len);
  if (fixed_key_len_) {
    TERARK_VERIFY_LE(int(pref_len_), fixed_key_len_ - 8);
  }
  live_iter_num_ = 0;
  TERARK_VERIFY_LE(sizeof(MetaInfo), indexBlock.data.size_);
  size_t num = table_properties_->num_entries;
  if (fixed_key_len_) {
    TERARK_VERIFY_GT(fixed_key_len_, 0);
    size_t fixed_ukey_len = fixed_key_len_ - 8 - this->pref_len_;
    byte_t* pool = base;
    if (fixed_value_len_ >= 0) {
      new(&fstrvec_) FixedLenStrVec(fixed_ukey_len + fixed_value_len_);
    } else {
      new(&fstrvec_) FixedLenStrVec(fixed_ukey_len + sizeof(uint32_t));
      pool = base + align_up(table_properties_->raw_value_size, 64);
    }
    fstrvec_.m_strpool.risk_set_data(pool, fstrvec_.m_fixlen * num);
    fstrvec_.m_size = num;
    fstrvec_.optimize_func();
  }
  else {
    size_t sum_suffix_len = table_properties_->raw_key_size - (pref_len_ + 8) * num;
    if (fixed_value_len_ >= 0) {
      new(&ssvec_) SortedStrVec;
      size_t offset = align_up(fixed_value_len_ * num, 64);
      size_t bits = ssvec_.m_offsets.compute_uintbits(sum_suffix_len);
      ssvec_.m_strpool.risk_set_data(base + offset, sum_suffix_len);
      offset += align_up(sum_suffix_len, 64);
      ssvec_.m_offsets.risk_set_data(base + offset, num + 1, bits);
      TERARK_VERIFY_EQ(sstmeta.var_key_offsets_len, ssvec_.m_offsets.mem_size());
    }
    else {
      size_t offset = 0;
      #define RiskSet(ptr, len) \
        ptr = base + offset; offset += pow2_align_up(len, 64)
      RiskSet(vkvv_.val_base, table_properties_->raw_value_size);
      RiskSet(vkvv_.key_base, sum_suffix_len);
      vkvv_.kvoffsets.risk_set_data((KeyValueOffset*)(base + offset), num + 1);
    }
  }
  if (sstmeta.version > 1) {
    THROW_STD(invalid_argument, "MetaInfo::version = %d", sstmeta.version);
  }
}

VecAutoSortTableReader::~VecAutoSortTableReader() {
  TERARK_VERIFY_F(0 == live_iter_num_, "real: %zd", live_iter_num_);
}

Status
VecAutoSortTableFactory::NewTableReader(
            const ReadOptions& ro,
            const TableReaderOptions& tro,
            std::unique_ptr<RandomAccessFileReader>&& file,
            uint64_t file_size,
            std::unique_ptr<TableReader>* table,
            bool prefetch_index_and_filter_in_cache)
const try {
  (void)prefetch_index_and_filter_in_cache; // now ignore
  Slice file_data;
  file->exchange(new MmapReadWrapper(file));
  Status s = TopMmapReadAll(*file, file_size, &file_data);
  if (!s.ok()) {
    return s;
  }
  MmapAdvSeq(file_data);
//MmapWarmUp(file_data);
  auto t = new VecAutoSortTableReader(this);
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

void VecAutoSortTableFactory::Update(const json&, const json& js, const SidePluginRepo&) {
  ROCKSDB_JSON_OPT_SIZE(js, fileWriteBufferSize);
  ROCKSDB_JSON_OPT_PROP(js, collectProperties);
  ROCKSDB_JSON_OPT_PROP(js, forceNeedCompact);
  ROCKSDB_JSON_OPT_PROP(js, accurateKeyAnchorsSize);
  ROCKSDB_JSON_OPT_SIZE(js, keyAnchorSizeUnit);
  ROCKSDB_JSON_OPT_PROP(js, debugLevel);
  ROCKSDB_JSON_OPT_PROP(js, minUserKeyLen);
}

VecAutoSortTableFactory::~VecAutoSortTableFactory() { // NOLINT
}

std::string VecAutoSortTableFactory::GetPrintableOptions() const {
  SidePluginRepo* repo = nullptr;
  return ToString({}, *repo);
}

Status
VecAutoSortTableFactory::ValidateOptions(const DBOptions& db_opts,
                                      const ColumnFamilyOptions& cf_opts)
const {
  if (!IsBytewiseComparator(cf_opts.comparator)) {
    return Status::InvalidArgument(ROCKSDB_FUNC, "comparator is not bytewise");
  }
  TERARK_VERIFY_EZ(cf_opts.comparator->timestamp_size());
  return Status::OK();
}

ROCKSDB_REG_Plugin("VecAutoSortTable", VecAutoSortTableFactory, TableFactory);
ROCKSDB_RegTableFactoryMagicNumber(kVecAutoSortTableMagic, "VecAutoSortTable");

std::string VecAutoSortTableFactory::ToString(const json &dump_options,
                                           const SidePluginRepo&) const {
  json djs;
  bool html = JsonSmartBool(dump_options, "html");
  ROCKSDB_JSON_SET_SIZE(djs, fileWriteBufferSize);
  ROCKSDB_JSON_SET_PROP(djs, collectProperties);
  ROCKSDB_JSON_SET_PROP(djs, forceNeedCompact);
  ROCKSDB_JSON_SET_PROP(djs, accurateKeyAnchorsSize);
  ROCKSDB_JSON_SET_SIZE(djs, keyAnchorSizeUnit);
  ROCKSDB_JSON_SET_PROP(djs, debugLevel);
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
  auto add_stats = [&](const char* name, FixedLenStats& flstats) {
    valvec<std::pair<int,int> > fixed_snap(flstats.size(), valvec_reserve());
    flstats.mtx.lock();
    for (size_t i = 0; i < flstats.end_i(); i++) {
      fixed_snap.push_back(flstats.elem_at(i));
    }
    flstats.mtx.unlock();
    json& fvjs = djs[name];
    for (auto& [len, cnt] : fixed_snap) {
      fvjs.push_back({{"Length", len}, {"Count", cnt}});
    }
    if (html && !fixed_snap.empty()) {
      fvjs[0]["<htmltab:col>"] = json::array({ "Length", "Count" });
    }
  };
  add_stats("FixedKey", fixed_key_stats);
  add_stats("FixedValue", fixed_value_stats);
  JS_TopTable_AddVersion(djs, html);
  JS_ToplingDB_AddVersion(djs, html);
  return JsonToString(djs, dump_options);
}

ROCKSDB_REG_EasyProxyManip("VecAutoSortTable", VecAutoSortTableFactory, TableFactory);
} // namespace rocksdb
