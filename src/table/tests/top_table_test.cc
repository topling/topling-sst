// Unit tests for Top Tables — shared TableFactory semantics and SimpleTop layout coverage.
//
// Template instantiations (Reader):
//   RecAt/UkeyAt/LowerBound/EqualRange/Get/Iter  ×  kFixedKey×kFixedValue
//     (T,T) dual-fixed | (T,F) key-fixed val-var | (F,T) key-var val-fixed | (F,F) dual-var
//   Iter × kWithGlobalSeqno {false=flush, true=ingest zero-seq SST}
// Runtime branches: reverse comparator, multi-version, deletion Get,
//   FinishAsEmptyTable, lazy kv_offsets_/keylens_ backfill transitions.
// Extra (BBT-inspired): empty/special key, MultiGet, iterate bounds,
//   ApproximateSizes, ApproximateKeyAnchors, Snapshot, RangeDelete,
//   randomized sets — each × layouts.
// debugLevel=2 → DebugCheckTable (iter/Seek/Get) on each built SST.

#include <rocksdb/db.h>
#include <rocksdb/file_checksum.h>
#include <rocksdb/metadata.h>
#include <rocksdb/options.h>
#include <rocksdb/slice.h>
#include <rocksdb/sst_file_writer.h>
#include <rocksdb/table.h>
#include <rocksdb/table_properties.h>

#include <topling/side_plugin_factory.h>
#include <topling/side_plugin_repo.h>

#include "port/stack_trace.h"
#include "test_util/testharness.h"
#include "util/file_checksum_helper.h"

#include <algorithm>
#include <functional>
#include <map>
#include <memory>
#include <random>
#include <stdio.h>
#include <stdlib.h>
#include <string>
#include <utility>
#include <vector>

namespace rocksdb {

using KV = std::pair<std::string, std::string>;

enum class Layout : int { kFF = 0, kTF, kFT, kDual };
enum class BuildPath : int { kFlush = 0, kIngest };

static const char* LayoutName(Layout L) {
  switch (L) {
    case Layout::kFF: return "ff";
    case Layout::kTF: return "tf";
    case Layout::kFT: return "ft";
    case Layout::kDual: return "dual";
  }
  return "?";
}

static void DestroyDBDir(const std::string& dbname) {
  // Avoid Options{}: cross-DSO delete of CacheAlignedNewDelete objects aborts.
  std::string cmd = "rm -rf '" + dbname + "'";
  int rc = system(cmd.c_str());
  (void)rc;
}

static std::shared_ptr<TableFactory> MakeFactory(SidePluginRepo& repo, const char* factory_name = "SimpleTopTable") {
  json js;
  js["debugLevel"] = 2;
  if (std::string(factory_name) == "ToplingZipTable") {
    js["debugLevel"] = 0;
    js["builderMinLevel"] = -1;
  }
  auto fac = PluginFactorySP<TableFactory>::AcquirePlugin(factory_name, js, repo);
  EXPECT_TRUE(fac != nullptr);
  return fac;
}

static Options MakeOptions(SidePluginRepo& repo, bool reverse, const char* factory_name = "SimpleTopTable") {
  Options options;
  options.create_if_missing = true;
  options.error_if_exists = false;
  options.comparator =
      reverse ? ReverseBytewiseComparator() : BytewiseComparator();
  options.allow_mmap_reads = true;
  options.level0_file_num_compaction_trigger = 100;
  options.disable_auto_compactions = true;
  options.table_factory = MakeFactory(repo, factory_name);
  options.compression = kNoCompression;
  options.num_levels = 2;
  options.write_buffer_size = 64 << 20;
  return options;
}

static std::string FmtKeyFixed(size_t i) {
  char b[16];
  snprintf(b, sizeof b, "k%04zu", i);
  return b;
}

static std::string FmtKeyVar(size_t i) {
  // variable ukey length, unique & bytewise-sortable by content
  return std::string(1 + (i % 5), char('a' + (i % 26))) + FmtKeyFixed(i);
}

static std::string FmtValFixed(size_t i) {
  char b[16];
  snprintf(b, sizeof b, "v%04zu", i);
  return b;
}

static std::string FmtValVar(size_t i) {
  return std::string(1 + (i % 9), 'x');
}

// Build n KV pairs that force one of the 4 layout templates after Finish.
static std::vector<KV> MakeLayoutKVs(Layout L, size_t n) {
  std::vector<KV> kvs;
  kvs.reserve(n);
  for (size_t i = 0; i < n; ++i) {
    switch (L) {
      case Layout::kFF:
        kvs.emplace_back(FmtKeyFixed(i), FmtValFixed(i));
        break;
      case Layout::kTF:
        kvs.emplace_back(FmtKeyFixed(i), FmtValVar(i));
        break;
      case Layout::kFT:
        kvs.emplace_back(FmtKeyVar(i), "VALU");
        break;
      case Layout::kDual:
        kvs.emplace_back(FmtKeyVar(i), FmtValVar(i));
        break;
    }
  }
  return kvs;
}

static void SortForComparator(std::vector<KV>* kvs, bool reverse) {
  const Comparator* cmp =
      reverse ? ReverseBytewiseComparator() : BytewiseComparator();
  std::sort(kvs->begin(), kvs->end(), [&](const KV& a, const KV& b) {
    return cmp->Compare(a.first, b.first) < 0;
  });
}

static void ForEachLayoutPath(
    const std::function<void(Layout, bool reverse, BuildPath)>& fn) {
  for (int li = 0; li < 4; ++li) {
    for (bool reverse : {false, true}) {
      for (BuildPath path : {BuildPath::kFlush, BuildPath::kIngest}) {
        fn(Layout(li), reverse, path);
      }
    }
  }
}

struct LoadedDB {
  SidePluginRepo repo;
  Options options;
  DB* db = nullptr;
  std::string dbname;
  std::string sst;
  std::vector<KV> kvs;  // comparator order for ingest; put order for flush

  ~LoadedDB() {
    delete db;
    DestroyDBDir(dbname);
    if (!sst.empty()) system(("rm -f '" + sst + "'").c_str());
  }
};

// Assert TableProperties land on the intended kFixedKey×kFixedValue template.
// Convention (same as Builder Finish): fixed_key_len is internal-key length;
// fixed_value_len == uint64_t(-1) means variable value.
static void ExpectLayout(DB* db, Layout L) {
  TablePropertiesCollection coll;
  ASSERT_OK(db->GetPropertiesOfAllTables(&coll));
  ASSERT_FALSE(coll.empty());
  bool found = false;
  for (auto it = coll.begin(); it != coll.end(); ++it) {
    const std::string fname = it->first.str();
    const auto& p = *it->second;
    if (p.compression_name != "SimpTop") continue;
    found = true;
    const bool key_fixed = p.fixed_key_len > 0;
    const bool val_fixed = p.fixed_value_len != uint64_t(-1);
    switch (L) {
      case Layout::kFF:
        ASSERT_TRUE(key_fixed) << fname;
        ASSERT_TRUE(val_fixed) << fname;
        break;
      case Layout::kTF:
        ASSERT_TRUE(key_fixed) << fname;
        ASSERT_FALSE(val_fixed) << fname;
        break;
      case Layout::kFT:
        ASSERT_FALSE(key_fixed) << fname;
        ASSERT_TRUE(val_fixed) << fname;
        break;
      case Layout::kDual:
        ASSERT_FALSE(key_fixed) << fname;
        ASSERT_FALSE(val_fixed) << fname;
        break;
    }
  }
  ASSERT_TRUE(found) << "no SimpTop SST in TablePropertiesCollection";
}

// Load KV set via Flush (kWithGlobalSeqno=false) or Ingest (true).
static void LoadDB(LoadedDB* L, const char* tag, Layout layout, bool reverse,
                   BuildPath path, std::vector<KV> kvs,
                   bool check_layout = true) {
  L->dbname = std::string("/tmp/simple_top_ut_") + tag + "_" +
              LayoutName(layout) + (reverse ? "_rev" : "") +
              (path == BuildPath::kIngest ? "_ing" : "_fl");
  L->sst = L->dbname + ".sst";
  DestroyDBDir(L->dbname);
  system(("rm -f '" + L->sst + "'").c_str());
  L->options = MakeOptions(L->repo, reverse);
  L->kvs = std::move(kvs);

  if (path == BuildPath::kIngest) {
    SortForComparator(&L->kvs, reverse);
    {
      SstFileWriter w(EnvOptions(), L->options);
      ASSERT_OK(w.Open(L->sst));
      for (auto& kv : L->kvs) {
        ASSERT_OK(w.Put(kv.first, kv.second));
      }
      ASSERT_OK(w.Finish());
    }
    ASSERT_OK(DB::Open(L->options, L->dbname, &L->db));
    IngestExternalFileOptions ifo;
    ifo.allow_global_seqno = true;
    ASSERT_OK(L->db->IngestExternalFile({L->sst}, ifo));
  } else {
    ASSERT_OK(DB::Open(L->options, L->dbname, &L->db));
    WriteOptions wo;
    for (auto& kv : L->kvs) {
      ASSERT_OK(L->db->Put(wo, kv.first, kv.second));
    }
    if (!L->kvs.empty()) {
      ASSERT_OK(L->db->Flush(FlushOptions()));
    }
  }
  if (check_layout) {
    ExpectLayout(L->db, layout);
  }
}

static void CheckIterate(DB* db, size_t expect_n) {
  ReadOptions ro;
  std::unique_ptr<Iterator> it(db->NewIterator(ro));
  size_t n = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) n++;
  ASSERT_OK(it->status());
  ASSERT_EQ(n, expect_n);
  size_t rn = 0;
  for (it->SeekToLast(); it->Valid(); it->Prev()) rn++;
  ASSERT_OK(it->status());
  ASSERT_EQ(rn, expect_n);
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    std::string k = it->key().ToString();
    std::string v = it->value().ToString();
    it->Seek(k);
    ASSERT_OK(it->status());
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(it->key(), k);
    ASSERT_EQ(it->value(), v);
    it->SeekForPrev(k);
    ASSERT_OK(it->status());
    ASSERT_TRUE(it->Valid());
    ASSERT_EQ(it->key(), k);
  }
  it->Seek("\xff\xff\xff\xff");
  ASSERT_OK(it->status());
}

static void CheckGets(DB* db, const std::vector<KV>& gets) {
  ReadOptions ro;
  for (auto& g : gets) {
    std::string val;
    Status s = db->Get(ro, g.first, &val);
    if (g.second == "__NOT_FOUND__") {
      ASSERT_TRUE(s.IsNotFound());
    } else {
      ASSERT_OK(s);
      ASSERT_EQ(val, g.second);
    }
  }
}

// Flush path → Iter/Get with kWithGlobalSeqno=false
static void RunFlushCase(const char* name, bool reverse, const std::vector<KV>& kvs,
                         const std::vector<KV>& gets) {
  printf("[%s]\n", name);
  SidePluginRepo repo;
  std::string dbname = std::string("/tmp/simple_top_smoke_") + name;
  DestroyDBDir(dbname);
  Options options = MakeOptions(repo, reverse);
  DB* db = nullptr;
  ASSERT_OK(DB::Open(options, dbname, &db));
  WriteOptions wo;
  for (auto& kv : kvs) {
    ASSERT_OK(db->Put(wo, kv.first, kv.second));
  }
  if (!kvs.empty()) {
    ASSERT_OK(db->Flush(FlushOptions()));
  }
  CheckIterate(db, kvs.size());
  CheckGets(db, gets);
  delete db;
  DestroyDBDir(dbname);
  printf("  PASS\n");
}

// Ingest zero-seq SST → Iter/Get with kWithGlobalSeqno=true (all 4 layouts)
static void RunIngestCase(const char* name, bool reverse, const std::vector<KV>& kvs,
                          const std::vector<KV>& gets) {
  printf("[%s]\n", name);
  SidePluginRepo repo;
  std::string dbname = std::string("/tmp/simple_top_smoke_") + name;
  std::string sst = dbname + ".sst";
  DestroyDBDir(dbname);
  system(("rm -f '" + sst + "'").c_str());

  Options options = MakeOptions(repo, reverse);
  {
    SstFileWriter w(EnvOptions(), options);
    ASSERT_OK(w.Open(sst));
    for (auto& kv : kvs) {
      ASSERT_OK(w.Put(kv.first, kv.second));
    }
    ASSERT_OK(w.Finish());
  }

  DB* db = nullptr;
  ASSERT_OK(DB::Open(options, dbname, &db));
  IngestExternalFileOptions ifo;
  ifo.allow_global_seqno = true;
  ASSERT_OK(db->IngestExternalFile({sst}, ifo));
  CheckIterate(db, kvs.size());
  CheckGets(db, gets);
  delete db;
  DestroyDBDir(dbname);
  system(("rm -f '" + sst + "'").c_str());
  printf("  PASS\n");
}

TEST(TopTableTest, Smoke) {
  // (T,T) dual-fixed empty value
  RunFlushCase("ff_empty", false,
               {{"ka", ""}, {"kb", ""}, {"kc", ""}},
               {{"ka", ""}, {"kb", ""}, {"kc", ""}, {"kz", "__NOT_FOUND__"}});
  RunFlushCase("ff_empty_rev", true,
               {{"kc", ""}, {"kb", ""}, {"ka", ""}},
               {{"ka", ""}, {"kb", ""}, {"kc", ""}});

  RunFlushCase("ff", false,
               {{"ka", "v1"}, {"kb", "v2"}, {"kc", "v3"}},
               {{"ka", "v1"}, {"kb", "v2"}, {"kc", "v3"}});
  RunFlushCase("ff_rev", true,
               {{"kc", "v3"}, {"kb", "v2"}, {"ka", "v1"}},
               {{"ka", "v1"}, {"kb", "v2"}, {"kc", "v3"}});

  RunFlushCase("tf", false,
               {{"k1", "a"}, {"k2", "bbbb"}, {"k3", "ccc"}},
               {{"k1", "a"}, {"k2", "bbbb"}, {"k3", "ccc"}});
  RunFlushCase("tf_rev", true,
               {{"k3", "ccc"}, {"k2", "bbbb"}, {"k1", "a"}},
               {{"k1", "a"}, {"k2", "bbbb"}, {"k3", "ccc"}});

  RunFlushCase("ft", false,
               {{"a", "vv"}, {"bb", "ww"}, {"ccc", "xx"}},
               {{"a", "vv"}, {"bb", "ww"}, {"ccc", "xx"}});
  RunFlushCase("ft_rev", true,
               {{"ccc", "xx"}, {"bb", "ww"}, {"a", "vv"}},
               {{"a", "vv"}, {"bb", "ww"}, {"ccc", "xx"}});

  RunFlushCase("tt_dual", false,
               {{"a", "x"}, {"bb", "yyyy"}, {"ccc", "z"}},
               {{"a", "x"}, {"bb", "yyyy"}, {"ccc", "z"}});
  RunFlushCase("tt_dual_rev", true,
               {{"ccc", "z"}, {"bb", "yyyy"}, {"a", "x"}},
               {{"a", "x"}, {"bb", "yyyy"}, {"ccc", "z"}});

  RunIngestCase("ingest_ff", false,
                {{"ka", "v1"}, {"kb", "v2"}, {"kc", "v3"}},
                {{"ka", "v1"}, {"kb", "v2"}, {"kc", "v3"}});
  RunIngestCase("ingest_tf", false,
                {{"k1", "a"}, {"k2", "bbbb"}, {"k3", "ccc"}},
                {{"k1", "a"}, {"k2", "bbbb"}, {"k3", "ccc"}});
  RunIngestCase("ingest_ft", false,
                {{"a", "vv"}, {"bb", "ww"}, {"ccc", "xx"}},
                {{"a", "vv"}, {"bb", "ww"}, {"ccc", "xx"}});
  RunIngestCase("ingest_dual", false,
                {{"a", "x"}, {"bb", "yyyy"}, {"ccc", "z"}},
                {{"a", "x"}, {"bb", "yyyy"}, {"ccc", "z"}});
  RunIngestCase("ingest_ff_rev", true,
                {{"kc", "v3"}, {"kb", "v2"}, {"ka", "v1"}},
                {{"ka", "v1"}, {"kb", "v2"}, {"kc", "v3"}});
  RunIngestCase("ingest_tf_rev", true,
                {{"k3", "ccc"}, {"k2", "bbbb"}, {"k1", "a"}},
                {{"k1", "a"}, {"k2", "bbbb"}, {"k3", "ccc"}});
  RunIngestCase("ingest_ft_rev", true,
                {{"ccc", "xx"}, {"bb", "ww"}, {"a", "vv"}},
                {{"a", "vv"}, {"bb", "ww"}, {"ccc", "xx"}});
  RunIngestCase("ingest_dual_rev", true,
                {{"ccc", "z"}, {"bb", "yyyy"}, {"a", "x"}},
                {{"a", "x"}, {"bb", "yyyy"}, {"ccc", "z"}});

  RunFlushCase("lazy_ff_to_tf", false,
               {{"k1", "aa"}, {"k2", "aa"}, {"k3", "bbbbbb"}},
               {{"k1", "aa"}, {"k2", "aa"}, {"k3", "bbbbbb"}});
  RunFlushCase("lazy_ff_to_ft", false,
               {{"aa", "v"}, {"bb", "v"}, {"ccc", "v"}},
               {{"aa", "v"}, {"bb", "v"}, {"ccc", "v"}});
  RunFlushCase("lazy_tf_to_dual", false,
               {{"k1", "a"}, {"k2", "bbbb"}, {"kkk", "c"}},
               {{"k1", "a"}, {"k2", "bbbb"}, {"kkk", "c"}});
  RunFlushCase("lazy_ft_to_dual", false,
               {{"a", "vv"}, {"bb", "vv"}, {"ccc", "wwww"}},
               {{"a", "vv"}, {"bb", "vv"}, {"ccc", "wwww"}});

  {
    printf("[multi_version]\n");
    SidePluginRepo repo;
    std::string dbname = "/tmp/simple_top_smoke_multi";
    DestroyDBDir(dbname);
    Options options = MakeOptions(repo, false);
    DB* db = nullptr;
    ASSERT_OK(DB::Open(options, dbname, &db));
    WriteOptions wo;
    ASSERT_OK(db->Put(wo, "mk", "old"));
    ASSERT_OK(db->Put(wo, "mk", "new"));
    ASSERT_OK(db->Put(wo, "nk", "only"));
    ASSERT_OK(db->Flush(FlushOptions()));
    CheckGets(db, {{"mk", "new"}, {"nk", "only"}});
    {
      ReadOptions ro;
      std::unique_ptr<Iterator> it(db->NewIterator(ro));
      size_t n = 0;
      for (it->SeekToFirst(); it->Valid(); it->Next()) n++;
      ASSERT_GE(n, 2u);
      it->Seek("mk");
      ASSERT_OK(it->status());
      ASSERT_TRUE(it->Valid());
      ASSERT_EQ(it->key().ToString(), "mk");
    }
    delete db;
    DestroyDBDir(dbname);
  }

  {
    printf("[deletion]\n");
    SidePluginRepo repo;
    std::string dbname = "/tmp/simple_top_smoke_del";
    DestroyDBDir(dbname);
    Options options = MakeOptions(repo, false);
    DB* db = nullptr;
    ASSERT_OK(DB::Open(options, dbname, &db));
    WriteOptions wo;
    ASSERT_OK(db->Put(wo, "dk", "alive"));
    ASSERT_OK(db->Delete(wo, "dk"));
    ASSERT_OK(db->Put(wo, "ek", "keep"));
    ASSERT_OK(db->Flush(FlushOptions()));
    CheckGets(db, {{"dk", "__NOT_FOUND__"}, {"ek", "keep"}});
    delete db;
    DestroyDBDir(dbname);
  }

  {
    printf("[empty_table]\n");
    SidePluginRepo repo;
    std::string dbname = "/tmp/simple_top_smoke_empty_tbl";
    DestroyDBDir(dbname);
    Options options = MakeOptions(repo, false);
    DB* db = nullptr;
    ASSERT_OK(DB::Open(options, dbname, &db));
    WriteOptions wo;
    ASSERT_OK(db->DeleteRange(wo, db->DefaultColumnFamily(), "a", "z"));
    ASSERT_OK(db->Flush(FlushOptions()));
    CheckGets(db, {{"m", "__NOT_FOUND__"}});
    {
      ReadOptions ro;
      std::unique_ptr<Iterator> it(db->NewIterator(ro));
      it->SeekToFirst();
      ASSERT_FALSE(it->Valid());
      ASSERT_OK(it->status());
    }
    delete db;
    DestroyDBDir(dbname);
  }

  {
    printf("[empty-db]\n");
    SidePluginRepo repo;
    std::string dbname = "/tmp/simple_top_smoke_empty";
    DestroyDBDir(dbname);
    Options options = MakeOptions(repo, false);
    DB* db = nullptr;
    ASSERT_OK(DB::Open(options, dbname, &db));
    delete db;
    DestroyDBDir(dbname);
  }
}

// ---------------------------------------------------------------------------
// BBT-inspired extras × all layout templates × reverse × flush/ingest
// ---------------------------------------------------------------------------

TEST(TopTableTest, EmptyAndSpecialKey) {
  // Empty ukey (var-key layouts) + same-length 0x00/0xff keys (fixed-key layouts).
  // Stresses LowerBound/EqualRange edges under both comparators.
  ForEachLayoutPath([&](Layout L, bool reverse, BuildPath path) {
    std::vector<KV> kvs;
    // NOTE: do not use {"\x00\x00", ...} — string(const char*) stops at NUL.
    const std::string k00("\x00\x00", 2);
    const std::string kff("\xff\xff", 2);
    switch (L) {
      case Layout::kFF:
        // fixed key len=2, fixed value len=4
        kvs = {{k00, "v000"}, {kff, "v001"}};
        break;
      case Layout::kTF:
        kvs = {{k00, "a"}, {kff, "bbbb"}};
        break;
      case Layout::kFT:
        kvs = {{"", "VALU"}, {kff, "VALU"}, {"mid", "VALU"}};
        break;
      case Layout::kDual:
        kvs = {{"", "x"}, {kff, "yyyy"}, {"m", "z"}};
        break;
    }
    LoadedDB loaded;
    LoadDB(&loaded, "edgekey", L, reverse, path, kvs);
    ASSERT_FALSE(HasFatalFailure());
    CheckIterate(loaded.db, kvs.size());
    for (auto& kv : kvs) {
      CheckGets(loaded.db, {kv});
      if (HasFatalFailure()) return;
    }
    CheckGets(loaded.db, {{"nope", "__NOT_FOUND__"}});
  });

  // dual-fixed empty values; empty ukey alone (var-key / fixed-value)
  for (bool reverse : {false, true}) {
    for (BuildPath path : {BuildPath::kFlush, BuildPath::kIngest}) {
      LoadedDB loaded;
      LoadDB(&loaded, "ff_empty_val", Layout::kFF, reverse, path,
             {{"ka", ""}, {"kb", ""}, {"kc", ""}});
      ASSERT_FALSE(HasFatalFailure());
      CheckIterate(loaded.db, 3);
      CheckGets(loaded.db, {{"ka", ""}, {"", "__NOT_FOUND__"}});
      if (HasFatalFailure()) return;

      LoadedDB loaded2;
      // single empty-ukey + fixed value → dual-fixed (one entry ⇒ key&val fixed)
      LoadDB(&loaded2, "empty_only", Layout::kFF, reverse, path,
             {{"", "VALU"}});
      ASSERT_FALSE(HasFatalFailure());
      CheckGets(loaded2.db, {{"", "VALU"}, {"x", "__NOT_FOUND__"}});
      if (HasFatalFailure()) return;
    }
  }
}

TEST(TopTableTest, MultiGetAllLayouts) {
  ForEachLayoutPath([&](Layout L, bool reverse, BuildPath path) {
    auto kvs = MakeLayoutKVs(L, 32);
    LoadedDB loaded;
    LoadDB(&loaded, "mget", L, reverse, path, kvs);
    ASSERT_FALSE(HasFatalFailure());

    std::map<std::string, std::string> expect;
    for (auto& kv : loaded.kvs) expect[kv.first] = kv.second;

    std::vector<Slice> keys;
    std::vector<std::string> key_store;
    // hit + miss interleaved
    key_store.push_back(loaded.kvs[0].first);
    key_store.emplace_back("___missing___");
    key_store.push_back(loaded.kvs[loaded.kvs.size() / 2].first);
    key_store.push_back(loaded.kvs.back().first);
    key_store.emplace_back("~~~missing~~~");
    for (auto& k : key_store) keys.emplace_back(k);

    std::vector<std::string> values;
    ReadOptions ro;
    std::vector<Status> statuses = loaded.db->MultiGet(ro, keys, &values);
    ASSERT_EQ(statuses.size(), keys.size());
    ASSERT_EQ(values.size(), keys.size());
    for (size_t i = 0; i < keys.size(); ++i) {
      auto it = expect.find(key_store[i]);
      if (it == expect.end()) {
        ASSERT_TRUE(statuses[i].IsNotFound());
      } else {
        ASSERT_OK(statuses[i]);
        ASSERT_EQ(values[i], it->second);
      }
      if (HasFatalFailure()) return;
    }
  });
}

TEST(TopTableTest, IterateBoundsAllLayouts) {
  ForEachLayoutPath([&](Layout L, bool reverse, BuildPath path) {
    auto kvs = MakeLayoutKVs(L, 16);
    LoadedDB loaded;
    LoadDB(&loaded, "bounds", L, reverse, path, kvs);
    ASSERT_FALSE(HasFatalFailure());

    // comparator-ordered view
    auto ordered = loaded.kvs;
    SortForComparator(&ordered, reverse);
    ASSERT_GE(ordered.size(), 4u);
    const std::string& lo = ordered[2].first;
    const std::string& hi = ordered[ordered.size() - 2].first;

    ReadOptions ro;
    Slice lower(lo);
    Slice upper(hi);
    ro.iterate_lower_bound = &lower;
    ro.iterate_upper_bound = &upper;
    std::unique_ptr<Iterator> it(loaded.db->NewIterator(ro));
    size_t n = 0;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ASSERT_OK(it->status());
      // [lower, upper)
      if (!reverse) {
        ASSERT_GE(it->key().compare(lower), 0);
        ASSERT_LT(it->key().compare(upper), 0);
      } else {
        // ReverseBytewise: lower/upper still mean user-key bounds in API;
        // DB enforces via comparator.
        ASSERT_TRUE(loaded.options.comparator->Compare(it->key(), lower) >= 0);
        ASSERT_TRUE(loaded.options.comparator->Compare(it->key(), upper) < 0);
      }
      ++n;
      if (HasFatalFailure()) return;
    }
    ASSERT_OK(it->status());
    ASSERT_GT(n, 0u);

    // Seek at/after upper → invalid (out of bound window)
    it->Seek(hi);
    ASSERT_OK(it->status());
    ASSERT_FALSE(it->Valid());
  });
}

TEST(TopTableTest, ApproximateSizesAllLayouts) {
  ForEachLayoutPath([&](Layout L, bool reverse, BuildPath path) {
    auto kvs = MakeLayoutKVs(L, 64);
    LoadedDB loaded;
    LoadDB(&loaded, "approx", L, reverse, path, kvs);
    ASSERT_FALSE(HasFatalFailure());

    auto ordered = loaded.kvs;
    SortForComparator(&ordered, reverse);
    uint64_t prev = 0;
    for (size_t i = 1; i < ordered.size(); ++i) {
      Range r;
      if (!reverse) {
        r.start = Slice();
        r.limit = ordered[i].first;
      } else {
        // reverse: sizes from "max" side — use full-range start
        static const char kMax[] = "\xff\xff\xff\xff\xff\xff\xff\xff";
        r.start = Slice(kMax, sizeof kMax - 1);
        r.limit = ordered[i].first;
      }
      uint64_t sz = 0;
      ASSERT_OK(loaded.db->GetApproximateSizes(&r, 1, &sz));
      ASSERT_GE(sz, prev);
      prev = sz;
      if (HasFatalFailure()) return;
    }
  });
}

TEST(TopTableTest, ApproximateKeyAnchorsAllLayouts) {
  // Exact range_size from record-pool offsets (not file_size/num average).
  ForEachLayoutPath([&](Layout L, bool reverse, BuildPath path) {
    constexpr size_t kN = 300;  // >128 ⇒ samples; also covers dual-var strides
    auto kvs = MakeLayoutKVs(L, kN);
    LoadedDB loaded;
    LoadDB(&loaded, "anchors", L, reverse, path, kvs);
    ASSERT_FALSE(HasFatalFailure());

    auto ordered = loaded.kvs;
    SortForComparator(&ordered, reverse);
    std::vector<size_t> ends;  // exclusive end offset after record i
    ends.reserve(ordered.size());
    size_t pool = 0;
    for (auto& kv : ordered) {
      pool += kv.first.size() + 8 + kv.second.size();  // ikey|value
      ends.push_back(pool);
    }

    std::vector<LiveFileMetaData> metas;
    loaded.db->GetLiveFilesMetaData(&metas);
    ASSERT_FALSE(metas.empty());
    Range full(metas[0].smallestkey, metas[0].largestkey);
    std::vector<Anchor> anchors;
    ASSERT_OK(loaded.db->ApproximateKeyAnchors(loaded.db->DefaultColumnFamily(),
                                               &full, &anchors));

    const size_t sum = ordered.size();
    const size_t num = std::min(sum, size_t{128});
    ASSERT_EQ(anchors.size(), num);
    const double step = double(sum) / num;
    size_t prev_off = 0;
    size_t sum_ranges = 0;
    for (size_t i = 0; i < num; ++i) {
      const size_t nth = std::min(size_t(step * (i + 1)), sum) - 1;
      const size_t curr_off = ends[nth];
      ASSERT_EQ(std::string(anchors[i].user_key), ordered[nth].first)
          << "layout=" << LayoutName(L) << " rev=" << reverse
          << " path=" << int(path) << " i=" << i;
      ASSERT_EQ(anchors[i].range_size, curr_off - prev_off)
          << "layout=" << LayoutName(L) << " rev=" << reverse
          << " path=" << int(path) << " i=" << i;
      sum_ranges += anchors[i].range_size;
      prev_off = curr_off;
      if (HasFatalFailure()) return;
    }
    ASSERT_EQ(sum_ranges, pool);
    ASSERT_EQ(prev_off, pool);
  });
}

TEST(TopTableTest, SnapshotMultiVersionAllLayouts) {
  // Snapshot must see old value; needs Flush path (memtable→SST with seqnos).
  for (int li = 0; li < 4; ++li) {
    for (bool reverse : {false, true}) {
      Layout L = Layout(li);
      // seed one other key so layout stays well-defined with multi-version
      auto base = MakeLayoutKVs(L, 4);
      LoadedDB loaded;
      loaded.dbname = std::string("/tmp/simple_top_ut_snap_") + LayoutName(L) +
                      (reverse ? "_rev" : "");
      DestroyDBDir(loaded.dbname);
      loaded.options = MakeOptions(loaded.repo, reverse);
      ASSERT_OK(DB::Open(loaded.options, loaded.dbname, &loaded.db));
      WriteOptions wo;
      const std::string& uk = base[1].first;
      ASSERT_OK(loaded.db->Put(wo, uk, "old_val_xx"));
      const Snapshot* snap = loaded.db->GetSnapshot();
      ASSERT_OK(loaded.db->Put(wo, uk, "new_val_yy"));
      for (size_t i = 0; i < base.size(); ++i) {
        if (base[i].first == uk) continue;
        ASSERT_OK(loaded.db->Put(wo, base[i].first, base[i].second));
      }
      ASSERT_OK(loaded.db->Flush(FlushOptions()));

      ReadOptions ro_snap;
      ro_snap.snapshot = snap;
      std::string val;
      ASSERT_OK(loaded.db->Get(ro_snap, uk, &val));
      ASSERT_EQ(val, "old_val_xx");
      ReadOptions ro;
      ASSERT_OK(loaded.db->Get(ro, uk, &val));
      ASSERT_EQ(val, "new_val_yy");
      loaded.db->ReleaseSnapshot(snap);
      if (HasFatalFailure()) return;
    }
  }
}

TEST(TopTableTest, RangeDeleteAllLayouts) {
  for (int li = 0; li < 4; ++li) {
    for (bool reverse : {false, true}) {
      Layout L = Layout(li);
      auto kvs = MakeLayoutKVs(L, 8);
      LoadedDB loaded;
      loaded.dbname = std::string("/tmp/simple_top_ut_rdel_") + LayoutName(L) +
                      (reverse ? "_rev" : "");
      DestroyDBDir(loaded.dbname);
      loaded.options = MakeOptions(loaded.repo, reverse);
      ASSERT_OK(DB::Open(loaded.options, loaded.dbname, &loaded.db));
      WriteOptions wo;
      for (auto& kv : kvs) {
        ASSERT_OK(loaded.db->Put(wo, kv.first, kv.second));
      }
      auto ordered = kvs;
      SortForComparator(&ordered, reverse);
      // DeleteRange [ordered[2], ordered[6]) in user-key space
      const std::string& begin = ordered[2].first;
      const std::string& end = ordered[6].first;
      ASSERT_OK(loaded.db->DeleteRange(wo, loaded.db->DefaultColumnFamily(),
                                       begin, end));
      ASSERT_OK(loaded.db->Flush(FlushOptions()));
      // range_del is meta-only; point KV lens unchanged → layout preserved
      ExpectLayout(loaded.db, L);

      ReadOptions ro;
      for (size_t i = 0; i < ordered.size(); ++i) {
        std::string val;
        Status s = loaded.db->Get(ro, ordered[i].first, &val);
        bool in_range =
            loaded.options.comparator->Compare(ordered[i].first, begin) >= 0 &&
            loaded.options.comparator->Compare(ordered[i].first, end) < 0;
        if (in_range) {
          ASSERT_TRUE(s.IsNotFound());
        } else {
          ASSERT_OK(s);
          ASSERT_EQ(val, ordered[i].second);
        }
        if (HasFatalFailure()) return;
      }
      std::unique_ptr<Iterator> it(loaded.db->NewIterator(ro));
      size_t alive = 0;
      for (it->SeekToFirst(); it->Valid(); it->Next()) ++alive;
      ASSERT_OK(it->status());
      ASSERT_EQ(alive, 4u);  // 8 - 4 deleted
    }
  }
}

class TopTableFactoryTest : public testing::TestWithParam<const char*> {};

TEST_P(TopTableFactoryTest, IngestRangeDeleteUsesGlobalSeqno) {
  const char* factory_name = GetParam();
  for (bool reverse : {false, true}) {
    for (bool empty_table : {false, true}) {
      LoadedDB loaded;
      loaded.dbname = std::string("/tmp/top_table_ut_ingest_rdel_") + factory_name + (reverse ? "_rev" : "") +
                      (empty_table ? "_empty" : "_table");
      loaded.sst = loaded.dbname + ".sst";
      DestroyDBDir(loaded.dbname);
      system(("rm -f '" + loaded.sst + "'").c_str());
      loaded.options = MakeOptions(loaded.repo, reverse, factory_name);
      ASSERT_OK(DB::Open(loaded.options, loaded.dbname, &loaded.db));

      ASSERT_OK(loaded.db->Put(WriteOptions(), "m", "old"));
      ASSERT_OK(loaded.db->Flush(FlushOptions()));
      const Snapshot* before_ingest = loaded.db->GetSnapshot();

      const char* outside_key = reverse ? "0" : "zz";
      {
        SstFileWriter writer(EnvOptions(), loaded.options);
        ASSERT_OK(writer.Open(loaded.sst));
        if (reverse) {
          ASSERT_OK(writer.DeleteRange("z", "a"));
        } else {
          ASSERT_OK(writer.DeleteRange("a", "z"));
        }
        if (!empty_table) {
          ASSERT_OK(writer.Put(outside_key, "outside"));
        }
        ASSERT_OK(writer.Finish());
      }

      IngestExternalFileOptions ifo;
      ifo.allow_global_seqno = true;
      ASSERT_OK(loaded.db->IngestExternalFile({loaded.sst}, ifo));

      std::string value;
      ASSERT_TRUE(loaded.db->Get(ReadOptions(), "m", &value).IsNotFound());
      if (!empty_table) {
        ASSERT_OK(loaded.db->Get(ReadOptions(), outside_key, &value));
        ASSERT_EQ(value, "outside");
      }

      // The tombstone was assigned after this snapshot, so the older value must still be visible through the snapshot.
      ReadOptions snapshot_read;
      snapshot_read.snapshot = before_ingest;
      ASSERT_OK(loaded.db->Get(snapshot_read, "m", &value));
      ASSERT_EQ(value, "old");
      loaded.db->ReleaseSnapshot(before_ingest);

      std::unique_ptr<Iterator> iter(loaded.db->NewIterator(ReadOptions()));
      size_t count = 0;
      for (iter->SeekToFirst(); iter->Valid(); iter->Next()) {
        ++count;
      }
      ASSERT_OK(iter->status());
      ASSERT_EQ(count, empty_table ? 0u : 1u);
    }
  }
}

TEST_P(TopTableFactoryTest, RangeDeleteKeepsStoredSeqno) {
  const char* factory_name = GetParam();
  for (bool reverse : {false, true}) {
    for (bool empty_table : {false, true}) {
      LoadedDB loaded;
      loaded.dbname = std::string("/tmp/top_table_ut_rdel_seq_") + factory_name + (reverse ? "_rev" : "") +
                      (empty_table ? "_empty" : "_table");
      DestroyDBDir(loaded.dbname);
      loaded.options = MakeOptions(loaded.repo, reverse, factory_name);
      ASSERT_OK(DB::Open(loaded.options, loaded.dbname, &loaded.db));

      ASSERT_OK(loaded.db->Put(WriteOptions(), "m", "old"));
      ASSERT_OK(loaded.db->Flush(FlushOptions()));
      const Snapshot* before_delete = loaded.db->GetSnapshot();
      ASSERT_OK(loaded.db->DeleteRange(WriteOptions(), loaded.db->DefaultColumnFamily(),
                                       reverse ? "z" : "a", reverse ? "a" : "z"));
      const Snapshot* after_delete = loaded.db->GetSnapshot();

      if (empty_table) {
        ASSERT_OK(loaded.db->DeleteRange(WriteOptions(), loaded.db->DefaultColumnFamily(),
                                         reverse ? "1" : "x", reverse ? "0" : "zz"));
      } else {
        ASSERT_OK(loaded.db->Put(WriteOptions(), reverse ? "0" : "zz", "outside"));
      }
      ASSERT_OK(loaded.db->Flush(FlushOptions()));

      std::string value;
      ReadOptions before_delete_read;
      before_delete_read.snapshot = before_delete;
      ASSERT_OK(loaded.db->Get(before_delete_read, "m", &value));
      ASSERT_EQ(value, "old");
      loaded.db->ReleaseSnapshot(before_delete);

      ReadOptions snapshot_read;
      snapshot_read.snapshot = after_delete;
      ASSERT_TRUE(loaded.db->Get(snapshot_read, "m", &value).IsNotFound());
      ASSERT_TRUE(loaded.db->Get(ReadOptions(), "m", &value).IsNotFound());
      loaded.db->ReleaseSnapshot(after_delete);
    }
  }
}

INSTANTIATE_TEST_CASE_P(
    TableFactories, TopTableFactoryTest,
#ifdef HAS_TOPLING_ROCKS
    testing::Values("SimpleTopTable", "SingleFastTable", "ToplingZipTable"));
#else
    testing::Values("SimpleTopTable", "SingleFastTable"));
#endif

TEST(TopTableTest, RandomizedGetIterAllLayouts) {
  ForEachLayoutPath([&](Layout L, bool reverse, BuildPath path) {
    const size_t n = 200;
    auto kvs = MakeLayoutKVs(L, n);
    // scramble put order for flush (ingest will re-sort)
    std::mt19937_64 rng(0x517e70u ^ (unsigned(L) * 17u) ^ (reverse ? 3u : 0u) ^
                        (path == BuildPath::kIngest ? 5u : 0u));
    std::shuffle(kvs.begin(), kvs.end(), rng);

    LoadedDB loaded;
    LoadDB(&loaded, "rand", L, reverse, path, kvs);
    ASSERT_FALSE(HasFatalFailure());

    std::map<std::string, std::string> expect;
    for (auto& kv : loaded.kvs) expect[kv.first] = kv.second;

    ReadOptions ro;
    for (auto& e : expect) {
      std::string val;
      ASSERT_OK(loaded.db->Get(ro, e.first, &val));
      ASSERT_EQ(val, e.second);
      if (HasFatalFailure()) return;
    }
    // random misses
    for (int i = 0; i < 20; ++i) {
      std::string miss = "miss_" + std::to_string(rng());
      std::string val;
      ASSERT_TRUE(loaded.db->Get(ro, miss, &val).IsNotFound());
    }

    std::unique_ptr<Iterator> it(loaded.db->NewIterator(ro));
    size_t cnt = 0;
    std::string prev;
    bool have_prev = false;
    for (it->SeekToFirst(); it->Valid(); it->Next()) {
      ASSERT_OK(it->status());
      auto f = expect.find(it->key().ToString());
      ASSERT_TRUE(f != expect.end());
      ASSERT_EQ(it->value().ToString(), f->second);
      if (have_prev) {
        ASSERT_LT(loaded.options.comparator->Compare(prev, it->key()), 0);
      }
      prev = it->key().ToString();
      have_prev = true;
      ++cnt;
      if (HasFatalFailure()) return;
    }
    ASSERT_EQ(cnt, expect.size());

    // backward
    cnt = 0;
    have_prev = false;
    for (it->SeekToLast(); it->Valid(); it->Prev()) {
      ASSERT_OK(it->status());
      if (have_prev) {
        ASSERT_GT(loaded.options.comparator->Compare(prev, it->key()), 0);
      }
      prev = it->key().ToString();
      have_prev = true;
      ++cnt;
    }
    ASSERT_EQ(cnt, expect.size());
  });
}

TEST(TopTableTest, SingleDeleteAllLayouts) {
  for (int li = 0; li < 4; ++li) {
    for (bool reverse : {false, true}) {
      Layout L = Layout(li);
      auto kvs = MakeLayoutKVs(L, 5);
      LoadedDB loaded;
      loaded.dbname = std::string("/tmp/simple_top_ut_sdel_") + LayoutName(L) +
                      (reverse ? "_rev" : "");
      DestroyDBDir(loaded.dbname);
      loaded.options = MakeOptions(loaded.repo, reverse);
      ASSERT_OK(DB::Open(loaded.options, loaded.dbname, &loaded.db));
      WriteOptions wo;
      for (auto& kv : kvs) {
        ASSERT_OK(loaded.db->Put(wo, kv.first, kv.second));
      }
      ASSERT_OK(loaded.db->SingleDelete(wo, kvs[2].first));
      ASSERT_OK(loaded.db->Flush(FlushOptions()));
      // tombstone keeps same key/value lens → layout template unchanged
      ExpectLayout(loaded.db, L);
      CheckGets(loaded.db, {{kvs[2].first, "__NOT_FOUND__"},
                            {kvs[0].first, kvs[0].second},
                            {kvs[1].first, kvs[1].second}});
      if (HasFatalFailure()) return;
    }
  }
}

TEST(TopTableTest, FileChecksumWhenFactorySet) {
  // Builder must force kRocksdbNative and emit a real file checksum.
  for (BuildPath path : {BuildPath::kFlush, BuildPath::kIngest}) {
    for (int li = 0; li < 4; ++li) {
      Layout L = Layout(li);
      SidePluginRepo repo;
      std::string dbname = std::string("/tmp/simple_top_ut_csum_") +
                           LayoutName(L) +
                           (path == BuildPath::kIngest ? "_ing" : "_fl");
      std::string sst = dbname + ".sst";
      DestroyDBDir(dbname);
      system(("rm -f '" + sst + "'").c_str());

      Options options = MakeOptions(repo, false);
      options.file_checksum_gen_factory = GetFileChecksumGenCrc32cFactory();
      auto kvs = MakeLayoutKVs(L, 8);

      if (path == BuildPath::kIngest) {
        ExternalSstFileInfo info;
        {
          SstFileWriter w(EnvOptions(), options);
          ASSERT_OK(w.Open(sst));
          for (auto& kv : kvs) {
            ASSERT_OK(w.Put(kv.first, kv.second));
          }
          ASSERT_OK(w.Finish(&info));
        }
        ASSERT_NE(info.file_checksum, kUnknownFileChecksum);
        ASSERT_FALSE(info.file_checksum.empty());
        ASSERT_NE(info.file_checksum_func_name, kUnknownFileChecksumFuncName);

        DB* db = nullptr;
        ASSERT_OK(DB::Open(options, dbname, &db));
        IngestExternalFileOptions ifo;
        ifo.allow_global_seqno = true;
        ifo.verify_file_checksum = true;
        ASSERT_OK(db->IngestExternalFile({sst}, ifo));
        ExpectLayout(db, L);
        ASSERT_OK(db->VerifyFileChecksums(ReadOptions()));
        delete db;
      } else {
        DB* db = nullptr;
        ASSERT_OK(DB::Open(options, dbname, &db));
        WriteOptions wo;
        for (auto& kv : kvs) {
          ASSERT_OK(db->Put(wo, kv.first, kv.second));
        }
        ASSERT_OK(db->Flush(FlushOptions()));
        ExpectLayout(db, L);

        std::vector<LiveFileMetaData> metas;
        db->GetLiveFilesMetaData(&metas);
        ASSERT_FALSE(metas.empty());
        bool any = false;
        for (auto& m : metas) {
          if (m.level < 0) continue;
          any = true;
          ASSERT_NE(m.file_checksum, kUnknownFileChecksum) << m.name;
          ASSERT_FALSE(m.file_checksum.empty()) << m.name;
          ASSERT_NE(m.file_checksum_func_name, kUnknownFileChecksumFuncName)
              << m.name;
        }
        ASSERT_TRUE(any);
        ASSERT_OK(db->VerifyFileChecksums(ReadOptions()));
        delete db;
      }
      DestroyDBDir(dbname);
      system(("rm -f '" + sst + "'").c_str());
      if (HasFatalFailure()) return;
    }
  }
}

}  // namespace rocksdb

int main(int argc, char** argv) {
  rocksdb::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
