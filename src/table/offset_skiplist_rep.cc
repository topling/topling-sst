//
// Created by leipeng on 2026-09-09
//
// OffsetSkipList MemTableRep — nodes live in ThreadCacheMemPool;
// next links are loc = byte_offset / AlignSize. Registered as SidePlugin,
// same pattern as CSPPMemTab.
//
// Value model matches CSPPMemTab: one user key -> one value vector
// (tag-sorted). CSPP stores trie value as loc(VecPin) so its value array
// can COW; this skiplist node *is* the vector header. In-order dups append
// in place when cap allows; out-of-order or full cap COW a new array so
// a published value array stays immutable (same as CSPP).
//
// ConvertToSST / memtable_as_log_index follow CSPPMemTab: dump the mempool
// as a custom SST (TopTable footer), and optionally store WAL refs instead
// of values.
//
#if defined(__clang__)
#pragma clang diagnostic ignored "-Wshorten-64-to-32"
#endif
#include <algorithm>
#include <cerrno>
#include <cstddef>
#include <cstdio>
#include <cstring>
#include <mutex>
#include <string>
#include <thread>
#include <utility>

#ifndef _MSC_VER
#include <unistd.h>
#endif

#include <db/dbformat.h>
#include <db/memtable.h>
#include <db/version_edit.h>
#include <file/filename.h>
#include <file/writable_file_writer.h>
#include <logging/logging.h>
#include <memory/arena.h>
#include <monitoring/iostats_context_imp.h>
#include <options/cf_options.h>
#include <rocksdb/memtablerep.h>
#include <rocksdb/write_batch.h>
#include <table/get_context.h>
#include <table/top_table_builder.h>
#include <table/top_table_reader.h>
#include <table/unique_id_impl.h>
#include <topling/builtin_table_factory.h>
#include <topling/side_plugin_factory.h>
#include <util/coding.h>

#include <terark/bitmanip.hpp>
#include <terark/fstring.hpp>
#include <terark/num_to_str.hpp>
#include <terark/offset_skiplist.hpp>
#include <terark/util/atomic.hpp>
#include <terark/valvec.hpp>

namespace rocksdb {

ROCKSDB_ENUM_CLASS(OSLConvertKind, uint8_t, kDontConvert, kDumpMem, kFileMmap);
ROCKSDB_ENUM_CLASS(OSLLogRefFormat, uint8_t, kNoLogRef, kPlainLogRef,
                   kShortLogRef);
static const uint64_t kOSLMemTabMagic = 0x62546d654d4c534fULL;  // OSLMemTb
struct OffsetSkipListFactory;

namespace {

using terark::as_atomic;
using terark::byte_t;
using terark::fstring;
using terark::string_appender;
#if !defined(terark_bsr_u32)
using terark::terark_bsr_u32;
#endif

constexpr uint32_t kAlign = 4;
constexpr uint32_t kLockFlag = uint32_t(1) << 31;
constexpr uint32_t kMetaVersion = 1;
const std::string kMetaName = "OffsetSkipList";

#pragma pack(push, 4)
struct OffsetSkipListMeta {
  uint32_t version;
  uint32_t log_ref;
  uint64_t mem_used;
  uint32_t head_loc;
  int32_t max_height;
  int32_t k_max_height;
  int32_t k_branching;
  uint64_t num_user_keys;  // on-disk name; value is num_nodes()
};
#pragma pack(pop)

static Slice NodeUkey(const char* p) { return Slice(p + 4, DecodeFixed32(p)); }

// Same 3-way Cmp split as Version::GetInst / DBIter::SetFuncPtr:
// forward bytewise, reverse bytewise, virtual fallback. No prefix cache.
struct FallbackUserKeySliceCmp {
  const Comparator* cmp = nullptr;
  FallbackUserKeySliceCmp(const Comparator* c = nullptr) : cmp(c) {}
  int operator()(Slice x, Slice y) const { return cmp->Compare(x, y); }
  bool equal(Slice x, Slice y) const { return cmp->Equal(x, y); }
};

template <class SliceCmp>
struct UserKeyCmp {
  SliceCmp slice_cmp;
  using DecodedType = Slice;
  Slice decode_key(const char* key) const { return NodeUkey(key); }
  int operator()(const char* a, const char* b) const {
    return slice_cmp(NodeUkey(a), NodeUkey(b));
  }
  int operator()(const char* a, const Slice& b) const {
    return slice_cmp(NodeUkey(a), b);
  }
  bool equal(const char* a, const char* b) const {
    return equal_ukey(slice_cmp, NodeUkey(a), NodeUkey(b));
  }
  bool equal(const char* a, const Slice& b) const {
    return equal_ukey(slice_cmp, NodeUkey(a), b);
  }

 private:
  static bool equal_ukey(const FallbackUserKeySliceCmp& c, Slice x, Slice y) {
    return c.equal(x, y);
  }
  template <class C>
  static bool equal_ukey(const C&, Slice x, Slice y) {
    return x == y;
  }
};

struct OffsetSkipListValueVec {
  uint32_t num;
  uint32_t pos;
};
using ValueVec = OffsetSkipListValueVec;

static size_t AlignUp4(size_t n) { return (n + 3) & ~size_t(3); }

static size_t EncValueLen(size_t raw) {
  return raw ? AlignUp4(VarintLength(uint32_t(raw)) + raw) : 0;
}

static void EncodePre(Slice d, void* buf) {
  char* p = EncodeVarint32(static_cast<char*>(buf), uint32_t(d.size()));
  memcpy(p, d.data(), d.size());
}

static ValueVec* NodeVec(const char* p) {
  return reinterpret_cast<ValueVec*>(const_cast<char*>(p)) - 1;
}

static uint32_t LoadUnlockedNum(const ValueVec* vec) {
  uint32_t num;
  while (kLockFlag & (num = as_atomic(vec->num).load(std::memory_order_acquire))) {
    std::this_thread::yield();
  }
  return num;
}

static uint32_t CapOf(uint32_t num) {
  return (num & (num - 1)) == 0 ? num : (2u << terark_bsr_u32(num));
}

class OffsetSkipListRep : public MemTableRep {
 public:
  virtual byte_t* base() const = 0;
  virtual const byte_t* sl_mem_data() const = 0;
  virtual size_t sl_mem_size() const = 0;
  virtual uint32_t sl_head_loc() const = 0;
  virtual int sl_max_height() const = 0;
  virtual int sl_k_max_height() const = 0;
  virtual int sl_k_branching() const = 0;
  virtual void FillMeta(OffsetSkipListMeta* m) const = 0;
  virtual Status SST_Get(const ReadOptions& ro, const ParsedInternalKey& pikey,
                         GetContext* get_context) const = 0;
  virtual bool GetRandomInternalKeysAppend(
      size_t num, std::vector<std::string>* output) const = 0;
  virtual uint64_t EstimateCountUkey(Slice ukey) const = 0;
  virtual uint64_t EstimateCountAll() const = 0;
  virtual void FillTableProperties(TableProperties* p) const = 0;
  virtual void MemGC() = 0;
  virtual const std::string& sl_mmap_fpath() const = 0;
  virtual fstring sl_get_mmap() const = 0;
  virtual void sl_set_readonly() = 0;
  virtual void FlushAllWalTls() = 0;
  void BindFactoryTokenOpts();

 protected:
  OffsetSkipListRep(Allocator* allocator, const Comparator* ucmp,
                    const SliceTransform* transform, size_t lookahead,
                    OffsetSkipListFactory* fac, Logger* log,
                    OSLConvertKind convert)
      : MemTableRep(allocator),
        ucmp_(ucmp),
        transform_(transform),
        lookahead_(lookahead),
        fac_(fac),
        log_(log),
        convert_to_sst_(convert) {}

 public:
#pragma pack(push, 4)
  struct KeyValueToMemRef {
    uint64_t tag;
    uint32_t pos;
    operator uint64_t() const noexcept { return tag; }
    Slice GetValue(const OffsetSkipListRep* tab) const noexcept {
      if (pos == 0) {
        return Slice();
      }
      auto* base = tab->base();
      return GetLengthPrefixedSlice(
          reinterpret_cast<const char*>(base + size_t(pos) * kAlign));
    }
  };
  struct KeyValueToLogRef {
    operator uint64_t() const noexcept { return tag; }
    uint64_t tag;
    union {
      struct {
        uint32_t val_len;
        uint64_t val_pos : 48;
        uint64_t wal_idx : 8;
        uint64_t inline_val_len : 8;
      };
      char value[11];
    };
    Slice GetValue(const OffsetSkipListRep* tab) const noexcept {
      if (inline_val_len <= sizeof(value)) {
        return {value, inline_val_len};
      }
      ROCKSDB_ASSERT_LT(wal_idx, tab->num_wals_);
      auto wal = tab->wals_[wal_idx].wal;
      return {wal->data_ + val_pos, val_len};
    }
  };
  static_assert(sizeof(KeyValueToLogRef) == 20, "");
  struct KV_ToShortLogRef {
    operator uint64_t() const noexcept { return tag; }
    uint64_t tag;
    union {
      struct {
        uint64_t val_pos : 48;
        uint64_t wal_idx : 8;
        uint64_t inline_val_len : 8;
      };
      char value[7];
    };
    Slice GetValue(const OffsetSkipListRep* tab) const noexcept {
      if (inline_val_len <= sizeof(value)) {
        return {value, inline_val_len};
      }
      ROCKSDB_ASSERT_LT(wal_idx, tab->num_wals_);
      auto wal = tab->wals_[wal_idx].wal;
      return GetLengthPrefixedSlice(wal->data_ + val_pos);
    }
  };
  static_assert(sizeof(KV_ToShortLogRef) == 16, "");
#pragma pack(pop)

  struct LogFileLookup {
    uint64_t fileno = 0;
    uint64_t cnt = 0;
    uint64_t bytes = 0;
    const ReadonlyFileMmap* wal = nullptr;
  };
  static constexpr size_t MAX_WALS = 16;

  const Comparator* ucmp_;
  const SliceTransform* transform_;
  const size_t lookahead_;
  OffsetSkipListFactory* fac_;
  Logger* log_;
  OSLConvertKind convert_to_sst_ = OSLConvertKind::kDontConvert;
  bool has_converted_to_sst_ = false;
  OSLLogRefFormat ref_to_wal_ = OSLLogRefFormat::kNoLogRef;
  bool token_use_idle_ = true;
  size_t num_wals_ = 0;
  LogFileLookup wals_[MAX_WALS] = {};
  std::mutex wal_mtx_;

  size_t add_wal(size_t fileno, const ReadonlyFileMmap* wal) {
    TERARK_ASSERT_NE(fileno, 0);
    size_t i = 0;
    while (i < num_wals_) {
      if (LIKELY(wals_[i].fileno == fileno)) {
        TERARK_ASSERT_EQ(wals_[i].wal, wal);
        return i;
      }
      i++;
    }
    std::lock_guard<std::mutex> lk(wal_mtx_);
    while (i < num_wals_) {
      if (wals_[i].fileno == fileno) {
        TERARK_VERIFY_EQ(wals_[i].wal, wal);
        return i;
      }
      i++;
    }
    TERARK_VERIFY_LT(num_wals_, MAX_WALS);
    wals_[i].cnt = 0;
    wals_[i].wal = wal;
    wals_[i].bytes = 0;
    wals_[i].fileno = fileno;
    as_atomic(num_wals_).fetch_add(1);
    intrusive_ptr_add_ref(const_cast<ReadonlyFileMmap*>(wal));
    return i;
  }

  void MarkReadOnly() override { FlushAllWalTls(); }

  void InitSetMemTableAsLogIndex(bool b) final;
  bool SupportMemTableAsLogIndex() const final {
    return ref_to_wal_ != OSLLogRefFormat::kNoLogRef;
  }
  bool SupportConvertToSST() const final {
    return convert_to_sst_ != OSLConvertKind::kDontConvert;
  }
  Status ConvertToSST(FileMetaData*, const TableBuilderOptions&) final;

  ~OffsetSkipListRep() override {
    for (size_t i = 0; i < num_wals_; ++i) {
      auto wal = wals_[i].wal;
      intrusive_ptr_release(const_cast<ReadonlyFileMmap*>(wal));
    }
  }
};

template <class SliceCmp>
class OffsetSkipListRepT final : public OffsetSkipListRep {
 public:
  using OffsetSL =
      terark::OffsetSkipList<UserKeyCmp<SliceCmp>, 4, OffsetSkipListValueVec>;
  OffsetSL skip_list_;

  struct Token : OffsetSL::Token {
    struct CntBytes {
      uint64_t cnt = 0;
      uint64_t bytes = 0;
    };
    CntBytes wal_cnt_[MAX_WALS] = {};

   protected:
    ~Token() override {
      auto* list = this->skiplist();
      if (!list) {
        return;
      }
      auto* tab = reinterpret_cast<OffsetSkipListRepT*>(
          reinterpret_cast<char*>(list) -
          offsetof(OffsetSkipListRepT, skip_list_));
      tab->ApplyWalToken(this);
    }
  };

  int CmpUkey(Slice a, Slice b) const { return SliceCmp{ucmp_}(a, b); }

  void ParkToken(typename OffsetSL::Token* tok) const {
    token_use_idle_ ? tok->idle() : tok->release();
  }
  void FinishHint(void* hint) override {
    if (hint == nullptr) {
      return;
    }
    auto* tok = static_cast<Token*>(hint);
    skip_list_.FinishHint(tok);
    ParkToken(tok);
  }

  OffsetSkipListRepT(const Comparator* ucmp, Allocator* allocator,
                     const SliceTransform* transform, const size_t lookahead,
                     size_t mem_cap, OffsetSkipListFactory* fac, Logger* log,
                     OSLConvertKind convert)
      : OffsetSkipListRep(allocator, ucmp, transform, lookahead, fac, log,
                          convert),
        skip_list_(UserKeyCmp<SliceCmp>{SliceCmp{ucmp}}, mem_cap) {}

  OffsetSkipListRepT(const Comparator* ucmp, Allocator* allocator,
                     const SliceTransform* transform, const size_t lookahead,
                     size_t mem_cap, OffsetSkipListFactory* fac, Logger* log,
                     OSLConvertKind convert, const std::string& mmap_path)
      : OffsetSkipListRep(allocator, ucmp, transform, lookahead, fac, log,
                          convert),
        skip_list_(UserKeyCmp<SliceCmp>{SliceCmp{ucmp}}, mem_cap, mmap_path) {}

  ~OffsetSkipListRepT() override {
    if (convert_to_sst_ == OSLConvertKind::kFileMmap &&
        !has_converted_to_sst_ && !skip_list_.mmap_fpath().empty()) {
      ::remove(skip_list_.mmap_fpath().c_str());
    }
  }

  OffsetSkipListRepT(const Comparator* ucmp, fstring mem, uint32_t head_loc,
                     int max_height, int32_t branching, uint64_t num_nodes,
                     OffsetSkipListFactory* fac, Logger* log)
      : OffsetSkipListRep(nullptr, ucmp, nullptr, 0, fac, log,
                          OSLConvertKind::kDontConvert),
        skip_list_(UserKeyCmp<SliceCmp>{SliceCmp{ucmp}}, mem, head_loc,
                   max_height, branching, num_nodes) {}

  byte_t* base() const final {
    return const_cast<byte_t*>(skip_list_.mem_data());
  }
  const byte_t* sl_mem_data() const final { return skip_list_.mem_data(); }
  size_t sl_mem_size() const final { return skip_list_.mem_size(); }
  uint32_t sl_head_loc() const final { return skip_list_.head_loc(); }
  int sl_max_height() const final { return skip_list_.max_height(); }
  int sl_k_max_height() const final { return skip_list_.k_max_height(); }
  int sl_k_branching() const final { return skip_list_.k_branching(); }
  const std::string& sl_mmap_fpath() const final {
    return skip_list_.mmap_fpath();
  }
  fstring sl_get_mmap() const final { return skip_list_.get_mmap(); }
  void sl_set_readonly() final { skip_list_.set_readonly(); }

  void ApplyWalToken(Token* tok) {
    for (size_t i = 0; i < num_wals_; ++i) {
      as_atomic(wals_[i].cnt)
          .fetch_add(tok->wal_cnt_[i].cnt, std::memory_order_relaxed);
      as_atomic(wals_[i].bytes)
          .fetch_add(tok->wal_cnt_[i].bytes, std::memory_order_relaxed);
      tok->wal_cnt_[i] = {};
    }
  }
  void FlushAllWalTls() final {
    skip_list_.for_each_tls_token([this](typename OffsetSL::Token* t) {
      if (auto* w = dynamic_cast<Token*>(t)) {
        ApplyWalToken(w);
      }
    });
  }
  template <class Entry>
  void AccountWal(Token* tok, size_t fidx, size_t valsize) {
    auto& x = tok->wal_cnt_[fidx];
    x.cnt++;
    x.bytes += valsize;
    constexpr size_t kWalTlsFlushBytes = 512 * 1024;
    size_t approx = x.cnt * sizeof(Entry) + x.bytes;
    if (UNLIKELY(approx > kWalTlsFlushBytes)) {
      as_atomic(wals_[fidx].cnt).fetch_add(x.cnt, std::memory_order_relaxed);
      as_atomic(wals_[fidx].bytes)
          .fetch_add(x.bytes, std::memory_order_relaxed);
      x = {};
    }
  }

  void MarkReadOnly() override {
    OffsetSkipListRep::MarkReadOnly();
    skip_list_.GCAll();
    skip_list_.set_readonly();
  }
  void MemGC() final { skip_list_.GCAll(); }

  template <class Entry>
  void AccountFirstInsert(Slice val, Token* tok) {
    if (val.size() == 0) {
      return;
    }
    ROCKSDB_VERIFY_EQ(val.size(), sizeof(KeyValuePassMemTable));
    auto* kv_pmt = reinterpret_cast<const KeyValuePassMemTable*>(val.data());
    auto valsize = kv_pmt->value.size_;
    if (valsize <= sizeof(static_cast<Entry*>(nullptr)->value)) {
      return;
    }
    auto fidx = add_wal(kv_pmt->fileno, kv_pmt->wal_file);
    AccountWal<Entry>(tok, fidx, valsize);
  }

  template <class Entry>
  void SetKeyValueToLogRef(Entry* entry, uint64_t tag, Slice val, Token* tok,
                           bool account) {
    entry->tag = tag;
    if (val.size() == 0) {
      memset(entry->value, 0, sizeof(entry->value) + 1);
      return;
    }
    ROCKSDB_VERIFY_EQ(val.size(), sizeof(KeyValuePassMemTable));
    auto* kv_pmt = reinterpret_cast<const KeyValuePassMemTable*>(val.data());
    auto valsize = kv_pmt->value.size_;
    if (valsize <= sizeof(entry->value)) {
      memset(entry->value, 0, sizeof(entry->value) + 1);
      memcpy(entry->value, kv_pmt->value.data_, valsize);
      entry->inline_val_len = valsize;
    } else {
      auto fidx = add_wal(kv_pmt->fileno, kv_pmt->wal_file);
      if (account) {
        AccountWal<Entry>(tok, fidx, valsize);
      }
      entry->wal_idx = fidx;
      if constexpr (std::is_same_v<Entry, KeyValueToLogRef>) {
        entry->val_pos = kv_pmt->val_pos;
        entry->val_len = uint32_t(valsize);
      } else {
        entry->val_pos = kv_pmt->val_pos - VarintLength(valsize);
      }
      entry->inline_val_len = 255;
    }
  }

  const char* FindNode(Slice ukey, typename OffsetSL::Token* tok) const {
    auto found = skip_list_.FindGreaterOrEqual(ukey, tok);
    if (found.first != nullptr && found.second == 0) {
      return found.first->Key();
    }
    return nullptr;
  }

  template <class Entry>
  void WriteFirstEntry(Entry* e, uint64_t tag, Slice val, size_t vpos,
                       Token* tok) {
    if constexpr (std::is_same_v<Entry, KeyValueToMemRef>) {
      e->tag = tag;
      if (val.size()) {
        EncodePre(val, base() + vpos);
        TERARK_ASSERT_AL(vpos, kAlign);
        e->pos = uint32_t(vpos / kAlign);
      } else {
        e->pos = 0;
      }
    } else {
      SetKeyValueToLogRef(e, tag, val, tok, /*account=*/false);
    }
  }

  template <class Entry>
  static size_t KeyPayloadSize(Slice ukey) {
    return AlignUp4(4 + ukey.size()) + sizeof(Entry);
  }

  template <class Entry>
  static size_t ValueLeadingSize(Slice val) {
    if constexpr (std::is_same_v<Entry, KeyValueToMemRef>) {
      return EncValueLen(val.size());
    }
    return 0;
  }

  template <class Entry>
  char* AllocNewNode(Slice ukey, uint64_t tag, Slice val, Token* tok,
                     size_t val_leading, size_t key_bytes) {
    const size_t entry_off_in_key = key_bytes - sizeof(Entry);
    char* p = skip_list_.AllocateKey(key_bytes, val_leading);
    TERARK_VERIFY_S(p != nullptr, "OffsetSkipList OOM: mem_cap=%zd",
                    skip_list_.mem_capacity());
    EncodeFixed32(p, uint32_t(ukey.size()));
    memcpy(p + 4, ukey.data(), ukey.size());
    auto* vec = NodeVec(p);
    auto* e = reinterpret_cast<Entry*>(p + entry_off_in_key);
    const size_t entry_off =
        reinterpret_cast<char*>(e) - reinterpret_cast<char*>(base());
    TERARK_ASSERT_AL(entry_off, kAlign);
    vec->pos = uint32_t(entry_off / kAlign);
    vec->num = 1;
    const size_t vpos =
        val_leading ? skip_list_.KeyAllocPos(p, val_leading) : 0;
    WriteFirstEntry(e, tag, val, vpos, tok);
    return p;
  }

  template <class Entry>
  bool InsertDup(char* node_key, uint64_t tag, Slice val, Token* token,
                 size_t reuse_vpos) {
    skip_list_.GC(token);
    auto* vec = NodeVec(node_key);
    const uint64_t curr_seq = tag >> 8;
    uint32_t vloc = 0;
    if constexpr (std::is_same_v<Entry, KeyValueToMemRef>) {
      if (reuse_vpos != size_t(-1)) {
        vloc = uint32_t(reuse_vpos / kAlign);
      } else if (val.size()) {
        size_t vpos = skip_list_.mempool().alloc(EncValueLen(val.size()));
        TERARK_VERIFY_NE(vpos, size_t(-1));
        EncodePre(val, base() + vpos);
        vloc = uint32_t(vpos / kAlign);
      }
    }
    auto write_entry = [&](Entry* e) {
      if constexpr (std::is_same_v<Entry, KeyValueToMemRef>) {
        e->tag = tag;
        e->pos = vloc;
      } else {
        SetKeyValueToLogRef(e, tag, val, token, /*account=*/true);
      }
    };
    auto free_val = [&]() {
      if constexpr (std::is_same_v<Entry, KeyValueToMemRef>) {
        if (vloc) {
          skip_list_.mempool().sfree(size_t(vloc) * kAlign,
                                     EncValueLen(val.size()));
        }
      }
    };
    size_t cow_pos = size_t(-1);
    uint32_t cow_cap = 0;
    auto free_cow = [&]() {
      if (cow_pos != size_t(-1)) {
        skip_list_.mempool().sfree(cow_pos, sizeof(Entry) * cow_cap);
        cow_pos = size_t(-1);
        cow_cap = 0;
      }
    };
    // Value / COW alloc stay outside the vec lock. Peek is only a size
    // guess; dup / append / COW are decided again under the lock.
    // Out-of-order never memmoves a published array (CSPP COW).
    for (;;) {
      const uint32_t snap_num = LoadUnlockedNum(vec);
      TERARK_ASSERT_GT(snap_num, 0);
      const uint32_t snap_cap = CapOf(snap_num);
      auto* snap_old =
          reinterpret_cast<Entry*>(base() + size_t(vec->pos) * kAlign);
      const uint64_t snap_last = snap_old[snap_num - 1].tag >> 8;
      const bool snap_append = snap_num < snap_cap && snap_last < curr_seq;
      bool snap_dup = snap_last == curr_seq;
      if (!snap_dup && snap_last > curr_seq) {
        size_t idx = terark::lower_bound_0(snap_old, snap_num, curr_seq << 8);
        snap_dup = snap_old[idx].tag >> 8 == curr_seq;
      }
      if (!snap_append && !snap_dup) {
        const uint32_t want =
            snap_num == snap_cap ? snap_cap * 2 : snap_cap;
        if (cow_pos == size_t(-1) || cow_cap != want) {
          free_cow();
          cow_pos = skip_list_.mempool().alloc(sizeof(Entry) * want);
          TERARK_VERIFY_NE(cow_pos, size_t(-1));
          cow_cap = want;
        }
      }

      uint32_t num;
      while (kLockFlag & (num = as_atomic(vec->num).fetch_or(
                              kLockFlag, std::memory_order_acquire))) {
        std::this_thread::yield();
      }
      const uint32_t old_cap = CapOf(num);
      TERARK_ASSERT_GT(num, 0);
      TERARK_ASSERT_LE(num, old_cap);
      auto* old = reinterpret_cast<Entry*>(base() + size_t(vec->pos) * kAlign);
      const uint64_t last_seq = old[num - 1].tag >> 8;
      if (UNLIKELY(curr_seq == last_seq)) {
        as_atomic(vec->num).store(num, std::memory_order_release);
        free_cow();
        free_val();
        return false;
      }
      if (num < old_cap && last_seq < curr_seq) {
        write_entry(&old[num]);
        as_atomic(vec->num).store(num + 1, std::memory_order_release);
        free_cow();
        return true;
      }
      const uint32_t want = num == old_cap ? old_cap * 2 : old_cap;
      if (cow_pos == size_t(-1) || cow_cap != want) {
        as_atomic(vec->num).store(num, std::memory_order_release);
        continue;
      }
      auto* neu = reinterpret_cast<Entry*>(base() + cow_pos);
      if (LIKELY(last_seq < curr_seq)) {
        memcpy(neu, old, sizeof(Entry) * num);
        write_entry(&neu[num]);
      } else {
        size_t idx = terark::lower_bound_0(old, num, curr_seq << 8);
        if (UNLIKELY(old[idx].tag >> 8 == curr_seq)) {
          as_atomic(vec->num).store(num, std::memory_order_release);
          free_cow();
          free_val();
          return false;
        }
        memcpy(neu, old, sizeof(Entry) * idx);
        write_entry(&neu[idx]);
        memcpy(neu + idx + 1, old + idx, sizeof(Entry) * (num - idx));
      }
      const size_t old_pos = size_t(vec->pos) * kAlign;
      vec->pos = uint32_t(cow_pos / kAlign);
      as_atomic(vec->num).store(num + 1, std::memory_order_release);
      skip_list_.LazyFree(old_pos, sizeof(Entry) * old_cap, token);
      return true;
    }
  }

  template <class Entry, bool Concurrent>
  bool InsertKVTpl(uint64_t tag, const Slice& ukey, const Slice& val,
                   void** hint) {
    auto* tok = skip_list_.template tls_token_nn<Token>();
    bool need_acquire = true;
    if (hint != nullptr) {
      auto*& slot = *reinterpret_cast<Token**>(hint);
      if (LIKELY(slot != nullptr)) {
        TERARK_ASSERT_EQ(slot, tok);
        if constexpr (Concurrent) {
          need_acquire = false;
        }
      } else {
        slot = tok;
      }
    }
    if (need_acquire) {
      tok->acquire(&skip_list_);
    }
    const size_t val_leading = ValueLeadingSize<Entry>(val);
    const size_t key_bytes = KeyPayloadSize<Entry>(ukey);
    char* node = AllocNewNode<Entry>(ukey, tag, val, tok, val_leading, key_bytes);
    const char* exist;
    if (hint != nullptr) {
      if constexpr (Concurrent) {
        exist = skip_list_.InsertWithHintConcurrently(node, tok);
      } else {
        exist = skip_list_.InsertWithHint(node, tok);
      }
    } else {
      if constexpr (Concurrent) {
        exist = skip_list_.InsertConcurrently(node, tok);
      } else {
        exist = skip_list_.Insert(node, tok);
      }
    }
    // Concurrent + hint: later inserts skip acquire. Otherwise park after.
    // FinishHint resets height; ~Token deletes the splice.
    const bool park = (hint == nullptr || !Concurrent);
    if (LIKELY(exist == nullptr)) {
      if constexpr (!std::is_same_v<Entry, KeyValueToMemRef>) {
        AccountFirstInsert<Entry>(val, tok);
      }
      if (park) {
        ParkToken(tok);
      }
      return true;
    }
    size_t reuse_vpos = size_t(-1);
    if (val_leading) {
      reuse_vpos =
          skip_list_.FreeUnusedKeyKeepLeading(node, key_bytes, val_leading);
    } else {
      skip_list_.FreeUnusedKey(node, key_bytes);
    }
    const bool dup_ok =
        InsertDup<Entry>(const_cast<char*>(exist), tag, val, tok, reuse_vpos);
    if (park) {
      ParkToken(tok);
    }
    return dup_ok;
  }

  template <bool Concurrent>
  bool InsertKV(uint64_t tag, const Slice& ukey, const Slice& val,
                void** hint) {
    if (ref_to_wal_ == OSLLogRefFormat::kPlainLogRef) {
      return InsertKVTpl<KeyValueToLogRef, Concurrent>(tag, ukey, val, hint);
    }
    if (ref_to_wal_ == OSLLogRefFormat::kShortLogRef) {
      return InsertKVTpl<KV_ToShortLogRef, Concurrent>(tag, ukey, val, hint);
    }
    return InsertKVTpl<KeyValueToMemRef, Concurrent>(tag, ukey, val, hint);
  }

  void FillMeta(OffsetSkipListMeta* m) const final {
    memset(m, 0, sizeof(*m));
    m->version = kMetaVersion;
    m->log_ref = static_cast<uint32_t>(ref_to_wal_);
    m->mem_used = skip_list_.mem_size();
    m->head_loc = skip_list_.head_loc();
    m->max_height = skip_list_.max_height();
    m->k_max_height = skip_list_.k_max_height();
    m->k_branching = skip_list_.k_branching();
    m->num_user_keys = skip_list_.num_nodes();
  }

  KeyHandle Allocate(const size_t, char**) override { TERARK_DIE("Bad call"); }
  void Insert(KeyHandle) override { TERARK_DIE("Bad call"); }
  bool InsertKey(KeyHandle) override { TERARK_DIE("Bad call"); }
  void InsertWithHint(KeyHandle, void**) override { TERARK_DIE("Bad call"); }
  bool InsertKeyWithHint(KeyHandle, void**) override { TERARK_DIE("Bad call"); }
  void InsertWithHintConcurrently(KeyHandle, void**) override {
    TERARK_DIE("Bad call");
  }
  bool InsertKeyWithHintConcurrently(KeyHandle, void**) override {
    TERARK_DIE("Bad call");
  }
  void InsertConcurrently(KeyHandle) override { TERARK_DIE("Bad call"); }
  bool InsertKeyConcurrently(KeyHandle) override { TERARK_DIE("Bad call"); }

  bool InsertKeyValue(uint64_t tag, const Slice& ukey,
                      const Slice& val) override {
    return InsertKV<false>(tag, ukey, val, nullptr);
  }
  bool InsertKeyValueWithHint(uint64_t tag, const Slice& ukey, const Slice& val,
                              void** hint) override {
    return InsertKV<false>(tag, ukey, val, hint);
  }
  bool InsertKeyValueConcurrently(uint64_t tag, const Slice& ukey,
                                  const Slice& val) override {
    return InsertKV<true>(tag, ukey, val, nullptr);
  }
  bool InsertKeyValueWithHintConcurrently(uint64_t tag, const Slice& ukey,
                                          const Slice& val,
                                          void** hint) override {
    return InsertKV<true>(tag, ukey, val, hint);
  }

  bool Contains(const Slice& internal_key) const override {
    Slice ukey = ExtractUserKey(internal_key);
    uint64_t find_tag = DecodeFixed64(ukey.end());
    auto* tok = skip_list_.template tls_token_nn<Token>();
    tok->acquire(const_cast<OffsetSL*>(&skip_list_));
    const char* node = FindNode(ukey, tok);
    if (!node) {
      ParkToken(tok);
      return false;
    }
    auto* vec = NodeVec(node);
    uint32_t num = LoadUnlockedNum(vec);
    auto* p = base() + size_t(vec->pos) * kAlign;
    bool ret;
    if (ref_to_wal_ == OSLLogRefFormat::kPlainLogRef) {
      ret = terark::binary_search_0(reinterpret_cast<KeyValueToLogRef*>(p), num,
                                    find_tag);
    } else if (ref_to_wal_ == OSLLogRefFormat::kShortLogRef) {
      ret = terark::binary_search_0(reinterpret_cast<KV_ToShortLogRef*>(p), num,
                                    find_tag);
    } else {
      ret = terark::binary_search_0(reinterpret_cast<KeyValueToMemRef*>(p), num,
                                    find_tag);
    }
    ParkToken(tok);
    return ret;
  }

  size_t ApproximateMemoryUsage() override {
    size_t walsize = 0;
    for (size_t i = 0; i < num_wals_; ++i) {
      walsize += wals_[i].bytes;
    }
    return skip_list_.mem_size() + walsize;
  }

  bool NeedsUserKeyCompareInGet() const override { return false; }

  void Get(const ReadOptions& ro, const LookupKey& k, void* callback_args,
           bool (*callback_func)(void* arg, const KeyValuePair&)) override {
    ParsedInternalKey pik(k.internal_key());
    GetPIK(ro, pik, callback_args, callback_func);
  }

  template <class Entry>
  void GetPIKTpl(const ReadOptions& ro, const ParsedInternalKey& k,
                 void* callback_args,
                 bool (*callback_func)(void* arg, const KeyValuePair&)) {
    KeyValuePair key_val(k.user_key);
    auto* tok = skip_list_.template tls_token_nn<Token>();
    tok->acquire(&skip_list_);
    const char* node = FindNode(k.user_key, tok);
    if (!node) {
      ParkToken(tok);
      return;
    }
    auto* vec = NodeVec(node);
    size_t num = LoadUnlockedNum(vec);
    auto* entry = reinterpret_cast<Entry*>(base() + size_t(vec->pos) * kAlign);
    intptr_t idx = intptr_t(terark::upper_bound_0(entry, num, k.GetTag()));
    if (UNLIKELY(ro.just_check_key_exists)) {
      while (idx--) {
        uint64_t tag = entry[idx].tag;
        if ((tag & 255) == kTypeMerge) {
          tag = (tag & ~uint64_t(255)) | kTypeValue;
        }
        key_val.tag = tag;
        if (!callback_func(callback_args, key_val)) {
          break;
        }
      }
    } else {
      while (idx--) {
        key_val.tag = entry[idx].tag;
        key_val.value = entry[idx].GetValue(this);
        if (!callback_func(callback_args, key_val)) {
          break;
        }
      }
    }
    ParkToken(tok);
  }

  void GetPIK(const ReadOptions& ro, const ParsedInternalKey& k,
              void* callback_args,
              bool (*callback_func)(void* arg, const KeyValuePair&)) override {
    if (ref_to_wal_ == OSLLogRefFormat::kPlainLogRef) {
      return GetPIKTpl<KeyValueToLogRef>(ro, k, callback_args, callback_func);
    }
    if (ref_to_wal_ == OSLLogRefFormat::kShortLogRef) {
      return GetPIKTpl<KV_ToShortLogRef>(ro, k, callback_args, callback_func);
    }
    return GetPIKTpl<KeyValueToMemRef>(ro, k, callback_args, callback_func);
  }

  template <class Entry>
  Status SST_GetTpl(const ReadOptions& ro, ParsedInternalKey pikey,
                    GetContext* get_context) const {
    Status st;
    auto* tok = skip_list_.template tls_token_nn<Token>();
    tok->acquire(const_cast<OffsetSL*>(&skip_list_));
    const char* node = FindNode(pikey.user_key, tok);
    if (!node) {
      ParkToken(tok);
      return st;
    }
    const SequenceNumber find_tag = pikey.GetTag();
    Cleanable noop_pinner;
    Cleanable* pinner =
        ro.internal_is_in_pinning_section ? &noop_pinner : nullptr;
    auto* vec = NodeVec(node);
    size_t num = LoadUnlockedNum(vec);
    auto* entry = reinterpret_cast<Entry*>(base() + size_t(vec->pos) * kAlign);
    intptr_t idx = intptr_t(terark::upper_bound_0(entry, num, find_tag));
    if (ro.just_check_key_exists) {
      while (idx--) {
        uint64_t tag = entry[idx].tag;
        UnPackSequenceAndType(tag, &pikey.sequence, &pikey.type);
        if (pikey.type == kTypeMerge) {
          pikey.type = kTypeValue;
        }
        if (!get_context->SaveValue(pikey, "", pinner)) {
          break;
        }
      }
    } else {
      while (idx--) {
        uint64_t tag = entry[idx].tag;
        UnPackSequenceAndType(tag, &pikey.sequence, &pikey.type);
        Slice value = entry[idx].GetValue(this);
        if (!get_context->SaveValue(pikey, value, pinner)) {
          break;
        }
      }
    }
    ParkToken(tok);
    return st;
  }

  Status SST_Get(const ReadOptions& ro, const ParsedInternalKey& pikey,
                 GetContext* get_context) const final {
    if (ref_to_wal_ == OSLLogRefFormat::kPlainLogRef) {
      return SST_GetTpl<KeyValueToLogRef>(ro, pikey, get_context);
    }
    if (ref_to_wal_ == OSLLogRefFormat::kShortLogRef) {
      return SST_GetTpl<KV_ToShortLogRef>(ro, pikey, get_context);
    }
    return SST_GetTpl<KeyValueToMemRef>(ro, pikey, get_context);
  }

  uint64_t ApproximateNumEntries(const Slice& start_ikey,
                                 const Slice& end_ikey) override {
    auto* tok = skip_list_.template tls_token_nn<Token>();
    tok->acquire(const_cast<OffsetSL*>(&skip_list_));
    uint64_t start_count =
        skip_list_.EstimateCount(ExtractUserKey(start_ikey), tok);
    uint64_t end_count =
        skip_list_.EstimateCount(ExtractUserKey(end_ikey), tok);
    ParkToken(tok);
    return (end_count >= start_count) ? (end_count - start_count) : 0;
  }

  uint64_t EstimateCountUkey(Slice ukey) const override {
    auto* tok = skip_list_.template tls_token_nn<Token>();
    tok->acquire(const_cast<OffsetSL*>(&skip_list_));
    uint64_t n = skip_list_.EstimateCount(ukey, tok);
    ParkToken(tok);
    return n;
  }
  uint64_t EstimateCountAll() const override {
    if (skip_list_.is_readonly()) {
      return EstimateCountAllTpl<true>();
    }
    return EstimateCountAllTpl<false>();
  }
  template <bool ReadOnly>
  uint64_t EstimateCountAllTpl() const {
    typename OffsetSL::template IteratorTpl<ReadOnly> it(&skip_list_);
    it.SeekToLast();
    if (!it.Valid()) {
      return 0;
    }
    if constexpr (ReadOnly) {
      return skip_list_.EstimateCount(skip_list_.DecodeKey(it.key())) + 1;
    } else {
      return skip_list_.EstimateCount(skip_list_.DecodeKey(it.key()), &it) + 1;
    }
  }

  void FillTableProperties(TableProperties* p) const override {
    const size_t num_entries = p->num_entries;
    const size_t num_user_keys = skip_list_.num_nodes();
    p->tag_size = 8 * num_entries;
    const size_t vec_hdr = sizeof(ValueVec) * num_user_keys;
    if (ref_to_wal_ == OSLLogRefFormat::kPlainLogRef) {
      p->data_size =
          vec_hdr + (sizeof(KeyValueToLogRef) - sizeof(uint64_t)) * num_entries;
    } else if (ref_to_wal_ == OSLLogRefFormat::kShortLogRef) {
      p->data_size =
          vec_hdr + (sizeof(KV_ToShortLogRef) - sizeof(uint64_t)) * num_entries;
    } else {
      p->data_size =
          p->raw_value_size + vec_hdr +
          (sizeof(KeyValueToMemRef) + 1 - sizeof(uint64_t)) * num_entries;
    }
    const size_t used =
        skip_list_.mem_size() - skip_list_.mempool().frag_size();
    const size_t rest = p->data_size + p->tag_size;
    p->index_size = used > rest ? used - rest : 0;
  }

  // Same sampling as InlineSkipList::FindRandomEntry / Iterator::RandomSeek.
  bool GetRandomInternalKeysAppend(
      size_t num, std::vector<std::string>* output) const override {
    if (skip_list_.is_readonly()) {
      return GetRandomInternalKeysAppendTpl<true>(num, output);
    }
    return GetRandomInternalKeysAppendTpl<false>(num, output);
  }
  template <bool ReadOnly>
  bool GetRandomInternalKeysAppendTpl(size_t num,
                                      std::vector<std::string>* output) const {
    typename OffsetSL::template IteratorTpl<ReadOnly> it(&skip_list_);
    const size_t old = output->size();
    for (size_t i = 0; i < num; ++i) {
      it.RandomSeek();
      if (!it.Valid()) {
        break;
      }
      Slice uk = NodeUkey(it.key());
      auto* vec = NodeVec(it.key());
      uint32_t n = LoadUnlockedNum(vec);
      if (n == 0) {
        continue;
      }
      uint64_t tag;
      memcpy(&tag, base() + size_t(vec->pos) * kAlign, sizeof(tag));
      output->emplace_back();
      output->back().assign(uk.data(), uk.size());
      PutFixed64(&output->back(), tag);
    }
    return output->size() > old;
  }

  template <class Entry, bool ReadOnly>
  class Iter : public MemTableRep::Iterator {
   public:
    explicit Iter(const OffsetSkipListRepT& rep)
        : rep_(rep),
          iter_(&rep.skip_list_),
          lookahead_(rep.lookahead_) {}
    ~Iter() override = default;

    bool Valid() const override { return idx_ >= 0; }
    const char* varlen_key() const override { TERARK_DIE("Bad call"); }
    Slice user_key() const override {
      TERARK_ASSERT_GE(idx_, 0);
      return Slice(ikey_.data(), ikey_.size() - 8);
    }
    Slice key() const override {
      TERARK_ASSERT_GE(idx_, 0);
      return Slice(ikey_);
    }
    Slice value() const override {
      TERARK_ASSERT_GE(idx_, 0);
      return Entries()[idx_].GetValue(&rep_);
    }
    std::pair<Slice, Slice> GetKeyValue() const override {
      return {key(), value()};
    }
    bool IsKeyPinned() const override { return false; }

    using MemTableRep::Iterator::Seek;
    using MemTableRep::Iterator::SeekForPrev;

    void Next() override { NextAndCheckValid(); }
    bool NextAndCheckValid() override {
      TERARK_ASSERT_GE(idx_, 0);
      if (idx_-- == 0) {
        bool advance_prev = true;
        if (lookahead_ && prev_key_) {
          Slice k1 = NodeUkey(prev_key_);
          Slice k2 = NodeUkey(iter_.key());
          if (k1.compare(k2) == 0) {
            advance_prev = false;
          } else if (rep_.transform_) {
            advance_prev = rep_.transform_->Transform(k1).compare(
                               rep_.transform_->Transform(k2)) == 0;
          }
        }
        if (advance_prev) {
          SavePrev();
        }
        iter_.Next();
        if (!iter_.Valid()) {
          idx_ = -1;
          return false;
        }
        LoadVec();
        idx_ = int(Num()) - 1;
      }
      SetIkey();
      return true;
    }

    void Prev() override { PrevAndCheckValid(); }
    bool PrevAndCheckValid() override {
      TERARK_ASSERT_GE(idx_, 0);
      if (++idx_ == int(Num())) {
        iter_.Prev();
        if (!iter_.Valid()) {
          idx_ = -1;
          return false;
        }
        LoadVec();
        idx_ = 0;
      }
      SavePrev();
      SetIkey();
      return true;
    }

    void Seek(const Slice& internal_key, const char* memtable_key) override {
      Slice ikey =
          memtable_key ? GetLengthPrefixedSlice(memtable_key) : internal_key;
      Slice ukey = ExtractUserKey(ikey);
      uint64_t find_tag = DecodeFixed64(ukey.end());
      if (lookahead_ && prev_key_ &&
          rep_.CmpUkey(NodeUkey(prev_key_), ukey) <= 0) {
        iter_.ResetKey(prev_key_);
        size_t cur = 0;
        while (cur++ <= lookahead_ && iter_.Valid()) {
          int c = rep_.CmpUkey(NodeUkey(iter_.key()), ukey);
          if (c >= 0) {
            LoadVec();
            if (c == 0) {
              LandEqual(find_tag);
            } else {
              idx_ = int(Num()) - 1;
              SetIkey();
            }
            SavePrev();
            return;
          }
          iter_.Next();
        }
      }
      iter_.Seek(ukey);
      if (!iter_.Valid()) {
        idx_ = -1;
        return;
      }
      LoadVec();
      if (rep_.CmpUkey(NodeUkey(iter_.key()), ukey) == 0) {
        LandEqual(find_tag);
      } else {
        idx_ = int(Num()) - 1;
        SetIkey();
      }
      SavePrev();
    }

    void SeekForPrev(const Slice& internal_key,
                     const char* memtable_key) override {
      Slice ikey =
          memtable_key ? GetLengthPrefixedSlice(memtable_key) : internal_key;
      Slice ukey = ExtractUserKey(ikey);
      uint64_t find_tag = DecodeFixed64(ukey.end());
      iter_.SeekForPrev(ukey);
      if (!iter_.Valid()) {
        idx_ = -1;
        return;
      }
      LoadVec();
      if (rep_.CmpUkey(NodeUkey(iter_.key()), ukey) == 0) {
        idx_ = int(terark::lower_bound_0(Entries(), Num(), find_tag));
        if (idx_ != int(Num())) {
          SetIkey();
          SavePrev();
          return;
        }
        iter_.Prev();
        if (!iter_.Valid()) {
          idx_ = -1;
          return;
        }
        LoadVec();
      }
      idx_ = 0;
      SetIkey();
      SavePrev();
    }

    void RandomSeek() override {
      iter_.RandomSeek();
      if (!iter_.Valid()) {
        idx_ = -1;
        return;
      }
      LoadVec();
      idx_ = int(Num()) - 1;
      SetIkey();
      SavePrev();
    }

    void SeekToFirst() override {
      iter_.SeekToFirst();
      if (!iter_.Valid()) {
        idx_ = -1;
        return;
      }
      LoadVec();
      idx_ = int(Num()) - 1;
      SetIkey();
      SavePrev();
    }

    void SeekToLast() override {
      iter_.SeekToLast();
      if (!iter_.Valid()) {
        idx_ = -1;
        return;
      }
      LoadVec();
      idx_ = 0;
      SetIkey();
      SavePrev();
    }

   private:
    uint32_t Num() const { return snap_num_; }
    Entry* Entries() const { return snap_entries_; }
    void SavePrev() { prev_key_ = iter_.Valid() ? iter_.key() : nullptr; }
    void LoadVec() {
      auto* vec = NodeVec(iter_.key());
      // Acquire num first so pos and [0, num) match one published array.
      snap_num_ = LoadUnlockedNum(vec);
      snap_entries_ = reinterpret_cast<Entry*>(
          rep_.base() + size_t(vec->pos) * kAlign);
    }
    void SetIkey() {
      Slice uk = NodeUkey(iter_.key());
      ikey_.assign(uk.data(), uk.size());
      PutFixed64(&ikey_, Entries()[idx_].tag);
    }
    void LandEqual(uint64_t find_tag) {
      idx_ = int(terark::upper_bound_0(Entries(), Num(), find_tag)) - 1;
      if (idx_ >= 0) {
        SetIkey();
        return;
      }
      iter_.Next();
      if (!iter_.Valid()) {
        idx_ = -1;
        return;
      }
      LoadVec();
      idx_ = int(Num()) - 1;
      SetIkey();
    }

    const OffsetSkipListRepT& rep_;
    typename OffsetSL::template IteratorTpl<ReadOnly> iter_;
    const char* prev_key_ = nullptr;
    Entry* snap_entries_ = nullptr;
    uint32_t snap_num_ = 0;
    int idx_ = -1;
    const size_t lookahead_;
    std::string ikey_;
  };

  template <class Entry, bool ReadOnly>
  static MemTableRep::Iterator* MakeIter(OffsetSkipListRepT* tab, Arena* a) {
    using It = Iter<Entry, ReadOnly>;
    void* mem = a ? a->AllocateAligned(sizeof(It)) : operator new(sizeof(It));
    return new (mem) It(*tab);
  }

  template <bool ReadOnly>
  MemTableRep::Iterator* MakeIterByEntry(Arena* arena) {
    if (ref_to_wal_ == OSLLogRefFormat::kPlainLogRef) {
      return MakeIter<KeyValueToLogRef, ReadOnly>(this, arena);
    }
    if (ref_to_wal_ == OSLLogRefFormat::kShortLogRef) {
      return MakeIter<KV_ToShortLogRef, ReadOnly>(this, arena);
    }
    return MakeIter<KeyValueToMemRef, ReadOnly>(this, arena);
  }

  MemTableRep::Iterator* GetIterator(Arena* arena = nullptr) override {
    if (skip_list_.is_readonly()) {
      return MakeIterByEntry<true>(arena);
    }
    return MakeIterByEntry<false>(arena);
  }
};

size_t ChooseMemCap(size_t configured, size_t write_buffer_size) {
  if (write_buffer_size == 0) {
    return configured;
  }
  auto require =
      std::min({write_buffer_size * 2, write_buffer_size + (size_t(1) << 30),
                size_t(16) << 30});
  return std::max(configured, require);
}

class OffsetSkipListTableBuilder : public TopTableBuilderBase {
 public:
  using TopTableBuilderBase::properties_;
  OffsetSkipListTableBuilder(const TableBuilderOptions& tbo,
                             WritableFileWriter* writer)
      : TopTableBuilderBase(tbo, writer) {
    offset_ = writer->GetFileSize();
  }
  void Add(const Slice&, const Slice&) final {
    ROCKSDB_DIE("Should not be called");
  }
  uint64_t EstimatedFileSize() const final {
    ROCKSDB_DIE("Should not be called");
  }
  Status Finish() final {
    closed_ = true;
    WriteMeta(
        kOSLMemTabMagic,
        {{kMetaName, WriteBlock(Slice(reinterpret_cast<const char*>(&meta_),
                                      sizeof(meta_)),
                                file_, &offset_)}});
    return Status::OK();
  }
  void Abandon() final { closed_ = true; }
  void DoWrite(Slice data) { WriteBlock(data, file_, &offset_); }
  void SetMeta(const OffsetSkipListMeta& m) { meta_ = m; }

 private:
  OffsetSkipListMeta meta_{};
};

static size_t SeekToEnd(WritableFileWriter& writer, Logger* log) {
  auto fs_file = writer.writable_file();
  auto fd = fs_file->FileDescriptor();
#ifndef _MSC_VER
  auto endpos = ::lseek(int(fd), 0, SEEK_END);
  if (endpos < 0) {
    std::string strerr = strerror(errno);
    std::string fname = writer.file_name().c_str();
    ROCKS_LOG_ERROR(log, "lseek(%s, 0, SEEK_END) = %s", fname.c_str(),
                    strerr.c_str());
    throw Status::IOError(fname, strerr);
  }
  fs_file->SetFileSize(endpos);
  writer.SetFileSize(endpos);
  return size_t(endpos);
#else
  (void)log;
  ROCKSDB_DIE("TODO");
  return 0;
#endif
}

template <class Fn>
OffsetSkipListRep* DispatchSliceCmp(const Comparator* uc, Fn&& fn) {
  if (uc->IsForwardBytewise()) {
    ROCKSDB_ASSERT_EQ(uc->timestamp_size(), 0);
    return fn(ForwardBytewiseCompareUserKeyNoTS{});
  }
  if (uc->IsReverseBytewise()) {
    ROCKSDB_ASSERT_EQ(uc->timestamp_size(), 0);
    return fn(ReverseBytewiseCompareUserKeyNoTS{});
  }
  return fn(FallbackUserKeySliceCmp{uc});
}

template <class... Args>
OffsetSkipListRep* NewOSLRep(const Comparator* uc, Args&&... args) {
  return DispatchSliceCmp(uc, [&](auto tag) -> OffsetSkipListRep* {
    using Cmp = decltype(tag);
    return new OffsetSkipListRepT<Cmp>(uc, std::forward<Args>(args)...);
  });
}

OffsetSkipListRep* NewOSLRepAttach(const Comparator* uc,
                                   const OffsetSkipListMeta* meta, byte_t* data,
                                   OffsetSkipListFactory* fac, Logger* log) {
  return DispatchSliceCmp(uc, [&](auto tag) -> OffsetSkipListRep* {
    using Cmp = decltype(tag);
    return new OffsetSkipListRepT<Cmp>(
        uc, fstring(reinterpret_cast<char*>(data), meta->mem_used),
        meta->head_loc, meta->max_height, meta->k_branching,
        meta->num_user_keys, fac, log);
  });
}

}  // namespace

struct OffsetSkipListFactory final : public MemTableRepFactory {
  size_t lookahead = 0;
  size_t mem_cap = 2LL << 30;
  OSLLogRefFormat log_ref_format = OSLLogRefFormat::kShortLogRef;
  OSLConvertKind convert_to_sst = OSLConvertKind::kDontConvert;
  bool token_use_idle = true;
  bool sync_sst_file = true;
  std::string chroot_dir;
  std::atomic<size_t> cumu_num{0};

  OffsetSkipListFactory(const json& js, const SidePluginRepo& r) {
    ROCKSDB_JSON_OPT_PROP(js, chroot_dir);
    Update({}, js, r);
  }

  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& cmp,
                                 Allocator* a, const SliceTransform* s,
                                 Logger* logger) final {
    return CreateMemTableRep("", MutableCFOptions(), cmp, a, s, logger, 0);
  }
  MemTableRep* CreateMemTableRep(const MemTableRep::KeyComparator& cmp,
                                 Allocator* a, const SliceTransform* s,
                                 Logger* logger, uint32_t cf_id) final {
    return CreateMemTableRep("", MutableCFOptions(), cmp, a, s, logger, cf_id);
  }
  MemTableRep* CreateMemTableRep(const std::string& level0_dir,
                                 const MutableCFOptions& mcfopt,
                                 const MemTableRep::KeyComparator& cmp,
                                 Allocator* allocator,
                                 const SliceTransform* transform,
                                 Logger* logger, uint32_t cf_id) final {
    auto cap = ChooseMemCap(mem_cap, mcfopt.write_buffer_size);
    auto convert = convert_to_sst;
    auto uc = cmp.icomparator()->user_comparator();
    if (convert == OSLConvertKind::kFileMmap) {
      auto idx = cumu_num.fetch_add(1, std::memory_order_relaxed);
      terark::string_appender<> path;
      path | chroot_dir | level0_dir;
      if (!path.empty() && path.end()[-1] != '/') {
        path | "/";
      }
      path ^ "OffsetSkipList-%06zd.memtab-" ^ idx ^ cf_id;
      auto* r = NewOSLRep(uc, allocator, transform, lookahead, cap, this,
                          logger, convert, path.str());
      r->BindFactoryTokenOpts();
      return r;
    }
    auto* r = NewOSLRep(uc, allocator, transform, lookahead, cap, this, logger,
                        convert);
    r->BindFactoryTokenOpts();
    return r;
  }

  const char* Name() const final { return "OffsetSkipList"; }
  bool IsInsertConcurrentlySupported() const final { return true; }
  bool CanHandleDuplicatedKey() const final { return true; }

  void Update(const json&, const json& js, const SidePluginRepo&) {
    ROCKSDB_JSON_OPT_PROP(js, lookahead);
    ROCKSDB_JSON_OPT_SIZE(js, mem_cap);
    ROCKSDB_JSON_OPT_ENUM(js, log_ref_format);
    ROCKSDB_JSON_OPT_ENUM(js, convert_to_sst);
    ROCKSDB_JSON_OPT_PROP(js, token_use_idle);
    ROCKSDB_JSON_OPT_PROP(js, sync_sst_file);
  }
  std::string ToString(const json& d, const SidePluginRepo&) const {
    return JsonToString(ToJson(d), d);
  }
  std::string GetPrintableOptions() const {
    return ToJson(json{}, false).dump();
  }
  json ToJson(const json& d) const { return ToJson(d, true); }
  json ToJson(const json& /*d*/, bool /*live_status*/) const {
    json djs;
    ROCKSDB_JSON_SET_PROP(djs, lookahead);
    ROCKSDB_JSON_SET_SIZE(djs, mem_cap);
    ROCKSDB_JSON_SET_ENUM(djs, log_ref_format);
    ROCKSDB_JSON_SET_ENUM(djs, convert_to_sst);
    ROCKSDB_JSON_SET_PROP(djs, token_use_idle);
    ROCKSDB_JSON_SET_PROP(djs, sync_sst_file);
    ROCKSDB_JSON_SET_PROP(djs, chroot_dir);
    return djs;
  }
};

void OffsetSkipListRep::BindFactoryTokenOpts() {
  token_use_idle_ = fac_->token_use_idle;
}

void OffsetSkipListRep::InitSetMemTableAsLogIndex(bool b) {
  ref_to_wal_ = b ? fac_->log_ref_format : OSLLogRefFormat::kNoLogRef;
}

Status OffsetSkipListRep::ConvertToSST(FileMetaData* meta,
                                       const TableBuilderOptions& tbo) try {
  FlushAllWalTls();
  MemGC();
  auto& ioptions = tbo.ioptions;
  auto* clock = ioptions.clock;
  auto* fs = ioptions.fs.get();
  ROCKSDB_VERIFY_NE(convert_to_sst_, OSLConvertKind::kDontConvert);
  {
    std::unique_ptr<MemTableRep::Iterator> probe(GetIterator(nullptr));
    probe->SeekToFirst();
    if (!probe->Valid()) {
      return Status::InvalidArgument("OffsetSkipList ConvertToSST: empty");
    }
  }
  bool sync_sst_file = fac_->sync_sst_file;
  IODebugContext dbg_ctx;
  FileOptions fopt;
  fopt.allow_fallocate = false;
  std::string fname = TableFileName(tbo.ioptions.cf_paths, meta->fd.GetNumber(),
                                    meta->fd.GetPathId());
  std::unique_ptr<FSWritableFile> fs_file;
  const bool is_file_mmap = convert_to_sst_ == OSLConvertKind::kFileMmap;
  double t0 = clock->NowMicros();
  OffsetSkipListMeta sst_meta;
  FillMeta(&sst_meta);
  if (is_file_mmap) {
    sl_set_readonly();
    std::string src_fname = sl_mmap_fpath();
    size_t chroot_len = fac_->chroot_dir.size();
    TERARK_VERIFY_S_EQ(fstring(src_fname).prefix(chroot_len), fac_->chroot_dir);
    src_fname.erase(0, chroot_len);
    IOStatus ios = fs->RenameFile(src_fname, fname, fopt.io_options, &dbg_ctx);
    if (!ios.ok()) {
      ROCKS_LOG_ERROR(log_, "rename(%s, %s) = %s", src_fname.c_str(),
                      fname.c_str(), ios.ToString().c_str());
      return ios;
    }
    ios = fs->ReopenWritableFile(fname, fopt, &fs_file, &dbg_ctx);
    if (!ios.ok()) {
      fs->DeleteFile(fname, fopt.io_options, &dbg_ctx);
      return ios;
    }
  } else {
    IOStatus ios = fs->NewWritableFile(fname, fopt, &fs_file, &dbg_ctx);
    if (!ios.ok()) {
      return ios;
    }
  }
  fs_file->SetPreallocationBlockSize(0);
  double t1 = clock->NowMicros();
  WritableFileWriter writer(std::move(fs_file), fname, fopt, ioptions.clock,
                            nullptr, ioptions.statistics.get(),
                            ioptions.listeners);
  auto fail_after_open = [&](const Status& err) {
    writer.Close();
    fs->DeleteFile(fname, fopt.io_options, &dbg_ctx);
    return err;
  };
  if (is_file_mmap) {
    auto endpos = SeekToEnd(writer, log_);
    ROCKSDB_VERIFY_EQ(sl_get_mmap().size(), endpos);
    IOSTATS_ADD(bytes_written, endpos);
  }
  OffsetSkipListTableBuilder builder(tbo, &writer);
  builder.SetMeta(sst_meta);
  if (!is_file_mmap) {
    try {
      builder.DoWrite(
          Slice(reinterpret_cast<const char*>(sl_mem_data()), sl_mem_size()));
    } catch (const Status& s) {
      builder.Abandon();
      return fail_after_open(s);
    }
  }
  double t2 = clock->NowMicros();
  builder.properties_.num_data_blocks = 1;
  builder.properties_.num_entries = meta->num_entries;
  builder.properties_.num_deletions = meta->num_deletions;
  builder.properties_.num_range_deletions = meta->num_range_deletions;
  builder.properties_.num_merge_operands = meta->num_merges;
  builder.properties_.raw_key_size = meta->raw_key_size;
  builder.properties_.raw_value_size = meta->raw_value_size;
  if (ref_to_wal_ != OSLLogRefFormat::kNoLogRef) {
    auto& oss = static_cast<string_appender<>&>(
        builder.properties_.compression_options);
    oss.clear();
    if (ref_to_wal_ == OSLLogRefFormat::kPlainLogRef) {
      oss | "LogRef:Plain;";
    } else if (ref_to_wal_ == OSLLogRefFormat::kShortLogRef) {
      oss | "LogRef:Short;";
    } else {
      ROCKSDB_DIE("Unexpected ref_to_wal_ = %d", static_cast<int>(ref_to_wal_));
    }
    if (num_wals_) {
      TERARK_VERIFY_LE(num_wals_, MAX_WALS);
      meta->oldest_blob_file_number = UINT64_MAX;
      for (size_t i = 0; i < num_wals_; i++) {
        auto& e = wals_[i];
        auto blob_no = tbo.generate_file_no();
        auto walname = LogFileName(ioptions.GetWalDir(), e.fileno);
        auto refname = BlobFileName(ioptions.cf_paths[0].path, blob_no);
        IOStatus ios =
            fs->LinkFile(walname, refname, fopt.io_options, &dbg_ctx);
        if (!ios.ok()) {
          builder.Abandon();
          return fail_after_open(ios);
        }
        oss | blob_no | ":" | e.fileno | ":" | e.cnt | ":" | e.bytes | ",";
        tbo.add_blob_file({blob_no, e.cnt, e.bytes, "", ""});
        terark::minimize(meta->oldest_blob_file_number, blob_no);
      }
      oss.pop_back();
    }
  }
  Status s = builder.Finish();
  if (!s.ok()) {
    return fail_after_open(s);
  }
  double t3 = clock->NowMicros();
  std::unique_ptr<MemTableRep::Iterator> iter(GetIterator(nullptr));
  iter->SeekToFirst();
  if (!iter->Valid()) {
    return fail_after_open(
        Status::InvalidArgument("OffsetSkipList ConvertToSST: empty"));
  }
  meta->smallest.DecodeFrom(iter->key());
  iter->SeekToLast();
  if (!iter->Valid()) {
    return fail_after_open(
        Status::InvalidArgument("OffsetSkipList ConvertToSST: empty"));
  }
  meta->largest.DecodeFrom(iter->key());
  meta->fd.file_size = writer.GetFileSize();
  meta->tail_size = builder.GetTailSize();
  if (!tbo.db_id.empty() && !tbo.db_session_id.empty()) {
    if (!GetSstInternalUniqueId(tbo.db_id, tbo.db_session_id,
                                meta->fd.GetNumber(), &meta->unique_id)
             .ok()) {
      meta->unique_id = kNullUniqueId64x2;
    }
  }
  double t4 = clock->NowMicros();
  s = writer.Flush();
  double t5 = clock->NowMicros();
  if (!s.ok()) {
    return fail_after_open(s);
  }
  if (sync_sst_file) {
    s = writer.writable_file()->Fsync(fopt.io_options, &dbg_ctx);
    if (!s.ok()) {
      return fail_after_open(s);
    }
  }
  double t6 = clock->NowMicros();
  writer.Close();
  double t7 = clock->NowMicros();
  if (is_file_mmap) {
    has_converted_to_sst_ = true;
  }
  double fsize_mb = meta->fd.file_size / double(1 << 20);
  ROCKS_LOG_INFO(log_,
                 "OffsetSkipListRep::ConvertToSST(%s): fsize = %8.3f M, "
                 "time(ms): open: %.3f, %s: %.3f, finish: %.3f, meta: %.3f, "
                 "Flush: %.3f, sync: %.3f, close: %.3f, all: %.3f",
                 fname.c_str(), fsize_mb, (t1 - t0) / 1e3,
                 is_file_mmap ? "seek" : "write", (t2 - t1) / 1e3,
                 (t3 - t2) / 1e3, (t4 - t3) / 1e3, (t5 - t4) / 1e3,
                 (t6 - t5) / 1e3, (t7 - t6) / 1e3, (t7 - t0) / 1e3);
  return s;
} catch (const std::exception& ex) {
  return Status::Aborted(ex.what());
} catch (const Status& s) {
  return s;
}

class OffsetSkipListTableFactory : public TableFactory {
 public:
  OffsetSkipListTableFactory(const json& js, const SidePluginRepo& repo) {
    memtable_factory = std::make_shared<OffsetSkipListFactory>(js, repo);
  }
  const char* Name() const override { return "OffsetSkipListTable"; }
  using TableFactory::NewTableReader;
  Status NewTableReader(const ReadOptions&, const TableReaderOptions&,
                        std::unique_ptr<RandomAccessFileReader>&&,
                        uint64_t file_size, std::unique_ptr<TableReader>*,
                        bool prefetch_index_and_filter) const override;
  TableBuilder* NewTableBuilder(const TableBuilderOptions&,
                                WritableFileWriter*) const override {
    ROCKSDB_DIE("Should not be called");
  }
  std::string GetPrintableOptions() const final {
    json djs = memtable_factory->ToJson({});
    ROCKSDB_JSON_SET_PROP(djs, populate_read);
    return djs.dump();
  }
  Status ValidateOptions(const DBOptions&,
                         const ColumnFamilyOptions&) const final {
    return Status::OK();
  }
  bool IsDeleteRangeSupported() const override { return false; }
  void Update(const json& q, const json& js, const SidePluginRepo& repo) {
    ROCKSDB_JSON_OPT_PROP(js, populate_read);
    memtable_factory->Update(q, js, repo);
  }
  std::string ToString(const json& d, const SidePluginRepo&) const {
    json djs = memtable_factory->ToJson(d);
    ROCKSDB_JSON_SET_PROP(djs, populate_read);
    return JsonToString(djs, d);
  }
  std::shared_ptr<OffsetSkipListFactory> memtable_factory;
  bool populate_read = true;
};

class OffsetSkipListTableReader : public TopTableReaderBase {
 public:
  ~OffsetSkipListTableReader() override = default;
  OffsetSkipListTableReader(RandomAccessFileReader*, Slice file_data,
                            const TableReaderOptions&,
                            const OffsetSkipListTableFactory*);
  InternalIterator* NewIterator(const ReadOptions&,
                                const SliceTransform* /*prefix_extractor*/,
                                Arena* arena, bool /*skip_filters*/,
                                TableReaderCaller /*caller*/,
                                size_t /*compaction_readahead_size*/,
                                bool /*allow_unprepared_value*/) final {
    return memtab_->GetIterator(arena);
  }
  uint64_t ApproximateOffsetOf(ROCKSDB_8_X_COMMA(const ReadOptions&)
                                   const Slice& key,
                               TableReaderCaller) final {
    ROCKSDB_VERIFY_GE(key.size(), 8);
    uint64_t tot = memtab_->EstimateCountAll();
    if (tot == 0) {
      return 0;
    }
    uint64_t c = memtab_->EstimateCountUkey(ExtractUserKey(key));
    return c * file_data_.size() / tot;
  }
  uint64_t ApproximateSize(ROCKSDB_8_X_COMMA(const ReadOptions&)
                               const Slice& beg,
                           const Slice& end, TableReaderCaller) final {
    ROCKSDB_VERIFY_GE(beg.size(), 8);
    ROCKSDB_VERIFY_GE(end.size(), 8);
    uint64_t tot = memtab_->EstimateCountAll();
    if (tot == 0) {
      return 0;
    }
    uint64_t a = memtab_->EstimateCountUkey(ExtractUserKey(beg));
    uint64_t b = memtab_->EstimateCountUkey(ExtractUserKey(end));
    uint64_t d = a > b ? a - b : b - a;
    return d * file_data_.size() / tot;
  }
  bool GetRandomInternalKeysAppend(
      size_t num, std::vector<std::string>* output) const final {
    return memtab_->GetRandomInternalKeysAppend(num, output);
  }
  size_t ApproximateMemoryUsage() const final {
    return file_data_.size() + table_properties_->gdic_size;
  }
  Status Get(const ReadOptions& ro, const Slice& ikey, GetContext* get_context,
             const SliceTransform*, bool /*skip_filters*/) final {
    ROCKSDB_ASSERT_GE(ikey.size(), kNumInternalBytes);
    return memtab_->SST_Get(ro, ParsedInternalKey(ikey), get_context);
  }
  Status VerifyChecksum(const ReadOptions&, TableReaderCaller) final {
    return Status::OK();
  }
  bool IsMyFactory(const TableFactory* fac) const final {
    return fac && dynamic_cast<const OffsetSkipListTableFactory*>(fac);
  }
  std::string ToWebViewString(const json& dump_options) const final {
    json djs;
    auto log_ref_format = memtab_->ref_to_wal_;
    auto convert_to_sst = memtab_->convert_to_sst_;
    auto token_use_idle = memtab_->token_use_idle_;
    auto lookahead = memtab_->lookahead_;
    ROCKSDB_JSON_SET_ENUM(djs, log_ref_format);
    ROCKSDB_JSON_SET_ENUM(djs, convert_to_sst);
    ROCKSDB_JSON_SET_PROP(djs, token_use_idle);
    ROCKSDB_JSON_SET_PROP(djs, lookahead);

    OffsetSkipListMeta meta;
    memtab_->FillMeta(&meta);
    auto num_user_keys = meta.num_user_keys;
    auto mem_used = meta.mem_used;
    auto max_height = meta.max_height;
    auto k_max_height = meta.k_max_height;
    auto k_branching = meta.k_branching;
    auto head_loc = meta.head_loc;
    ROCKSDB_JSON_SET_PROP(djs, num_user_keys);
    ROCKSDB_JSON_SET_SIZE(djs, mem_used);
    ROCKSDB_JSON_SET_PROP(djs, max_height);
    ROCKSDB_JSON_SET_PROP(djs, k_max_height);
    ROCKSDB_JSON_SET_PROP(djs, k_branching);
    ROCKSDB_JSON_SET_PROP(djs, head_loc);

    const auto& tp = *table_properties_;
    auto num_entries = tp.num_entries;
    auto num_deletions = tp.num_deletions;
    auto num_merge_operands = tp.num_merge_operands;
    auto num_range_deletions = tp.num_range_deletions;
    auto raw_key_size = tp.raw_key_size;
    auto raw_value_size = tp.raw_value_size;
    auto tag_size = tp.tag_size;
    auto index_size = tp.index_size;
    auto data_size = tp.data_size;
    auto file_size = file_data_.size();
    ROCKSDB_JSON_SET_PROP(djs, num_entries);
    ROCKSDB_JSON_SET_PROP(djs, num_deletions);
    ROCKSDB_JSON_SET_PROP(djs, num_merge_operands);
    ROCKSDB_JSON_SET_PROP(djs, num_range_deletions);
    ROCKSDB_JSON_SET_SIZE(djs, raw_key_size);
    ROCKSDB_JSON_SET_SIZE(djs, raw_value_size);
    ROCKSDB_JSON_SET_SIZE(djs, tag_size);
    ROCKSDB_JSON_SET_SIZE(djs, index_size);
    ROCKSDB_JSON_SET_SIZE(djs, data_size);
    ROCKSDB_JSON_SET_SIZE(djs, file_size);

    if (factory_) {
      const auto& fac = *factory_->memtable_factory;
      ROCKSDB_JSON_SET_PROP(djs, fac.lookahead);
      ROCKSDB_JSON_SET_SIZE(djs, fac.mem_cap);
      ROCKSDB_JSON_SET_PROP(djs, fac.sync_sst_file);
      auto populate_read = factory_->populate_read;
      ROCKSDB_JSON_SET_PROP(djs, populate_read);
    }

    if (memtab_->num_wals_) {
      json& ref_to_wal = djs["ref_to_wal"];
      size_t sum_ref_cnt = 0, sum_ref_size = 0, sum_file_size = 0;
      for (size_t i = 0; i < memtab_->num_wals_; i++) {
        auto& e = memtab_->wals_[i];
        sum_ref_cnt += e.cnt;
        sum_ref_size += e.bytes;
        size_t wal_file_size = e.wal ? e.wal->size() : 0;
        sum_file_size += wal_file_size;
        json blobjs;
        blobjs["blob_file"] = e.wal ? e.wal->fileno : 0;
        blobjs["wal_file"] = e.fileno;
        blobjs["ref_cnt"] = e.cnt;
        blobjs["ref_size"] = SizeToString(e.bytes);
        blobjs["ref_avg"] = e.cnt ? e.bytes / double(e.cnt) : 0;
        blobjs["file_size"] = SizeToString(wal_file_size);
        blobjs["ref_ratio"] =
            wal_file_size ? 100.0 * e.bytes / wal_file_size : 0;
        ref_to_wal.push_back(std::move(blobjs));
      }
      if (memtab_->num_wals_ > 1) {
        ref_to_wal.push_back(json::object({
            {"blob_file", "sum"},
            {"wal_file", "sum"},
            {"ref_cnt", sum_ref_cnt},
            {"ref_size", SizeToString(sum_ref_size)},
            {"ref_avg", sum_ref_cnt ? sum_ref_size / double(sum_ref_cnt) : 0},
            {"file_size", SizeToString(sum_file_size)},
            {"ref_ratio",
             sum_file_size ? 100.0 * sum_ref_size / sum_file_size : 0},
        }));
      }
      ref_to_wal[0]["<htmltab:col>"] = json::array({
          "blob_file",
          "wal_file",
          "ref_cnt",
          "ref_size",
          "ref_avg",
          "file_size",
          "ref_ratio",
      });
      auto refwal_cnt = sum_ref_cnt;
      auto inline_cnt = num_entries - sum_ref_cnt;
      ROCKSDB_JSON_SET_PROP(djs, refwal_cnt);
      ROCKSDB_JSON_SET_PROP(djs, inline_cnt);
    }
    return JsonToString(djs, dump_options);
  }

  std::unique_ptr<OffsetSkipListRep> memtab_;
  const OffsetSkipListTableFactory* factory_ = nullptr;
};

OffsetSkipListTableReader::OffsetSkipListTableReader(
    RandomAccessFileReader* file, Slice file_data,
    const TableReaderOptions& tro, const OffsetSkipListTableFactory* f) {
  LoadCommonPart(file, tro, file_data, kOSLMemTabMagic);
  BlockContents meta_bc = ReadMetaBlockE(
      file, file_data.size(), kOSLMemTabMagic, tro.ioptions, kMetaName);
  TERARK_VERIFY_GE(meta_bc.data.size(), sizeof(OffsetSkipListMeta));
  auto* sst_meta =
      reinterpret_cast<const OffsetSkipListMeta*>(meta_bc.data.data());
  TERARK_VERIFY_EQ(sst_meta->version, kMetaVersion);
  TERARK_VERIFY_GE(file_data.size(), sst_meta->mem_used);
  memtab_.reset(NewOSLRepAttach(
      tro.ioptions.user_comparator, sst_meta,
      const_cast<byte_t*>(reinterpret_cast<const byte_t*>(file_data.data())),
      f->memtable_factory.get(), tro.ioptions.logger));
  memtab_->BindFactoryTokenOpts();
  memtab_->ref_to_wal_ = static_cast<OSLLogRefFormat>(sst_meta->log_ref);
  table_properties_->compression_name = "OffsetSkipList";
  std::string& compression_options = table_properties_->compression_options;
  if (Slice(compression_options).starts_with("LogRef:")) {
    const char* item = strchr(compression_options.c_str(), ';');
    ROCKSDB_VERIFY(item != nullptr);
    const fstring name(compression_options.c_str(), item);
    if (name == "LogRef:Plain") {
      memtab_->ref_to_wal_ = OSLLogRefFormat::kPlainLogRef;
    } else if (name == "LogRef:Short") {
      memtab_->ref_to_wal_ = OSLLogRefFormat::kShortLogRef;
    } else {
      ROCKSDB_DIE("Unexpected LogRef: %s", compression_options.c_str());
    }
    item += 1;
    for (size_t i = 0; true; i++) {
      size_t blob_no = 0, wal_no = 0, cnt = 0, bytes = 0;
      int fields =
          sscanf(item, "%zd:%zd:%zd:%zd", &blob_no, &wal_no, &cnt, &bytes);
      if (fields <= 0) {
        break;
      }
      ROCKSDB_ASSERT_EQ(fields, 4);
      if (4 != fields) {
        THROW_STD(logic_error, "must be blob_no:wal_no:cnt:bytes, but is: %s",
                  item);
      }
      auto fpath = BlobFileName(tro.ioptions.cf_paths[0].path, blob_no);
      auto [fmap, ios] =
          ReadonlyFileMmap::New(*tro.ioptions.fs, blob_no, fpath);
      TERARK_VERIFY_S(ios.ok(), "ReadonlyFileMmap %s, %s", fpath,
                      ios.ToString());
      memtab_->wals_[i].fileno = wal_no;
      memtab_->wals_[i].cnt = cnt;
      memtab_->wals_[i].wal = fmap.get();
      memtab_->wals_[i].bytes = bytes;
      memtab_->num_wals_++;
      TERARK_VERIFY_LE(memtab_->num_wals_, OffsetSkipListRep::MAX_WALS);
      intrusive_ptr_add_ref(fmap.get());
      table_properties_->gdic_size += bytes;
      item = strchr(item, ',');
      if (item) {
        item += 1;
      } else {
        break;
      }
    }
  }
  memtab_->FillTableProperties(table_properties_.get());
  factory_ = f;
}

Status OffsetSkipListTableFactory::NewTableReader(
    const ReadOptions&, const TableReaderOptions& tro,
    std::unique_ptr<RandomAccessFileReader>&& file, uint64_t file_size,
    std::unique_ptr<TableReader>* table, bool prefetch_index_and_filter) const
    try {
  (void)prefetch_index_and_filter;
  file->exchange(new MmapReadWrapper(file, populate_read));
  Slice file_data;
  Status s = TopMmapReadAll(*file, file_size, &file_data);
  if (!s.ok()) {
    return s;
  }
  if (!populate_read) {
    MmapAdvSeq(file_data);
    MmapWarmUp(file_data);
  }
  table->reset(
      new OffsetSkipListTableReader(file.release(), file_data, tro, this));
  return Status::OK();
} catch (const IOStatus& s) {
  return Status::IOError(ROCKSDB_FUNC, s.ToString());
} catch (const Status& s) {
  return s;
} catch (const std::exception& ex) {
  return Status::Corruption(ROCKSDB_FUNC, ex.what());
}

ROCKSDB_REG_Plugin("OffsetSkipList", OffsetSkipListFactory, MemTableRepFactory);
ROCKSDB_REG_EasyProxyManip("OffsetSkipList", OffsetSkipListFactory,
                           MemTableRepFactory);
ROCKSDB_REG_Plugin("OffsetSkipListTable", OffsetSkipListTableFactory,
                   TableFactory);
ROCKSDB_REG_EasyProxyManip("OffsetSkipListTable", OffsetSkipListTableFactory,
                           TableFactory);
ROCKSDB_RegTableFactoryMagicNumber(kOSLMemTabMagic, "OffsetSkipListTable");

static json EasyJsParams(Slice params) {
  if (params.empty()) {
    return json::object();
  }
  return json::parse(params.data(), params.data() + params.size());
}

std::shared_ptr<MemTableRepFactory> EasyNewMemTableRep(Slice class_name,
                                                       Slice params) {
  json js = EasyJsParams(params);
  const SidePluginRepo repo;
  return PluginFactorySP<MemTableRepFactory>::AcquirePlugin(
      class_name.ToString(), js, repo);
}

// OffsetSkipListTable only. MemTable factories use EasyNewMemTableRep →
// AcquirePlugin so cspp / OffsetSkipList share one path.
TableFactory* EasyNewTableFactory(Slice class_name, Slice params) {
  json js = EasyJsParams(params);
  const SidePluginRepo repo;
  if (class_name == "OffsetSkipListTable") {
    return new OffsetSkipListTableFactory(js, repo);
  }
  THROW_InvalidArgument("EasyNewTableFactory: unknown class " +
                        class_name.ToString());
}

}  // namespace rocksdb
