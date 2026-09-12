#include <atomic>
#include <condition_variable>
#include <cstring>
#include <memory>
#include <mutex>
#include <numeric>
#include <set>
#include <type_traits>
#include <terark/offset_skiplist.hpp>
#include <thread>
#include <unordered_set>
#include <vector>

#include "db/blob/blob_file_addition.h"
#include "db/dbformat.h"
#include "db/lookup_key.h"
#include "db/memtable.h"
#include "db/merge_context.h"
#include "db/table_properties_collector.h"
#include "db/version_edit.h"
#include "file/filename.h"
#include "file/random_access_file_reader.h"
#include "memory/arena.h"
#include "options/cf_options.h"
#include "port/stack_trace.h"
#include "rocksdb/comparator.h"
#include "rocksdb/file_system.h"
#include "rocksdb/slice_transform.h"
#include "rocksdb/table.h"
#include "rocksdb/write_batch.h"
#include "rocksdb/write_buffer_manager.h"
#include "table/table_builder.h"
#include "table/table_reader.h"
#include "test_util/testharness.h"
#include "util/coding.h"
#include "util/hash.h"
#include "util/random.h"

namespace ROCKSDB_NAMESPACE {

using Key = uint64_t;

static const char* Encode(const uint64_t* key) {
  return reinterpret_cast<const char*>(key);
}

static Key Decode(const char* key) {
  Key rv;
  memcpy(&rv, key, sizeof(Key));
  return rv;
}

struct TestComparator {
  using DecodedType = Key;
  static DecodedType decode_key(const char* b) { return Decode(b); }
  int operator()(const char* a, const char* b) const {
    if (Decode(a) < Decode(b)) return -1;
    if (Decode(a) > Decode(b)) return +1;
    return 0;
  }
  int operator()(const char* a, const DecodedType b) const {
    if (Decode(a) < b) return -1;
    if (Decode(a) > b) return +1;
    return 0;
  }
  bool equal(const char* a, const char* b) const {
    return Decode(a) == Decode(b);
  }
  bool equal(const char* a, const DecodedType b) const {
    return Decode(a) == b;
  }
};

using TestOffsetSkipList = terark::OffsetSkipList<TestComparator, 4>;

static constexpr size_t kTestMemCap = size_t(16) << 20;

static TestOffsetSkipList::Token* EnsureAcquired(
    TestOffsetSkipList* list, TestOffsetSkipList::Token* tok = nullptr) {
  if (tok == nullptr) {
    tok = list->tls_token();
  }
  if (tok->state() != TestOffsetSkipList::AcquireDone) {
    tok->acquire(list);
  }
  return tok;
}

struct ScopedPin {
  TestOffsetSkipList* list;
  TestOffsetSkipList::Token* tok;
  bool owns;
  explicit ScopedPin(TestOffsetSkipList* l)
      : list(l), tok(l->tls_token()), owns(false) {
    if (tok->state() != TestOffsetSkipList::AcquireDone) {
      tok->acquire(l);
      owns = true;
    }
  }
  ~ScopedPin() {
    if (owns) {
      tok->release();
    }
  }
  ScopedPin(const ScopedPin&) = delete;
  ScopedPin& operator=(const ScopedPin&) = delete;
};

class OffsetSkipTest : public testing::Test {
 public:
  void Insert(TestOffsetSkipList* list, Key key) {
    char* buf = list->AllocateKey(sizeof(Key));
    ASSERT_NE(buf, nullptr);
    memcpy(buf, &key, sizeof(Key));
    ASSERT_EQ(list->Insert(buf, EnsureAcquired(list)), nullptr);
    keys_.insert(key);
  }

  bool InsertWithHint(TestOffsetSkipList* list, Key key,
                      TestOffsetSkipList::Token* token) {
    char* buf = list->AllocateKey(sizeof(Key));
    EXPECT_NE(buf, nullptr);
    memcpy(buf, &key, sizeof(Key));
    bool res =
        list->InsertWithHint(buf, EnsureAcquired(list, token)) == nullptr;
    keys_.insert(key);
    return res;
  }

  void Validate(TestOffsetSkipList* list) {
    ScopedPin pin(list);
    for (Key key : keys_) {
      ASSERT_TRUE(list->Contains(key, pin.tok));
      ASSERT_NE(list->Get(key, pin.tok), nullptr);
    }
    TestOffsetSkipList::Iterator iter(list);
    ASSERT_FALSE(iter.Valid());
    Key zero = 0;
    iter.Seek(zero);
    for (Key key : keys_) {
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(key, Decode(iter.key()));
      iter.Next();
    }
    ASSERT_FALSE(iter.Valid());
    list->TEST_Validate();
  }

 private:
  std::set<Key> keys_;
};

TEST_F(OffsetSkipTest, Empty) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  ScopedPin pin(&list);
  Key key = 10;
  ASSERT_TRUE(!list.Contains(key, pin.tok));
  ASSERT_EQ(list.Get(key, pin.tok), nullptr);
  ASSERT_EQ(list.mem_align_size(), 4U);
  ASSERT_GT(list.mem_capacity(), 0U);

  TestOffsetSkipList::Iterator iter(&list);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToFirst();
  ASSERT_TRUE(!iter.Valid());
  key = 100;
  iter.Seek(key);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekForPrev(key);
  ASSERT_TRUE(!iter.Valid());
  iter.SeekToLast();
  ASSERT_TRUE(!iter.Valid());
}

TEST_F(OffsetSkipTest, InsertAndLookup) {
  const int N = 2000;
  const int R = 5000;
  Random rnd(1000);
  std::set<Key> keys;
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  auto* tok = EnsureAcquired(&list);
  for (int i = 0; i < N; i++) {
    Key key = rnd.Next() % R;
    if (keys.insert(key).second) {
      char* buf = list.AllocateKey(sizeof(Key));
      ASSERT_NE(buf, nullptr);
      auto off = buf - reinterpret_cast<const char*>(list.mem_data());
      ASSERT_EQ(off % 4, 0);
      memcpy(buf, &key, sizeof(Key));
      ASSERT_EQ(list.Insert(buf, tok), nullptr);
    }
  }
  for (Key i = 0; i < R; i++) {
    if (list.Contains(i, tok)) {
      ASSERT_EQ(keys.count(i), 1U);
      ASSERT_NE(list.Get(i, tok), nullptr);
    } else {
      ASSERT_EQ(keys.count(i), 0U);
      ASSERT_EQ(list.Get(i, tok), nullptr);
    }
  }

  {
    TestOffsetSkipList::Iterator iter(&list);
    ASSERT_TRUE(!iter.Valid());
    uint64_t zero = 0;
    iter.Seek(zero);
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), Decode(iter.key()));

    uint64_t max_key = R - 1;
    iter.SeekForPrev(max_key);
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.rbegin()), Decode(iter.key()));

    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.begin()), Decode(iter.key()));

    iter.SeekToLast();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(*(keys.rbegin()), Decode(iter.key()));
  }

  for (Key i = 0; i < R; i++) {
    TestOffsetSkipList::Iterator iter(&list);
    iter.Seek(i);
    std::set<Key>::iterator model_iter = keys.lower_bound(i);
    for (int j = 0; j < 3; j++) {
      if (model_iter == keys.end()) {
        ASSERT_TRUE(!iter.Valid());
        break;
      }
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(*model_iter, Decode(iter.key()));
      ++model_iter;
      iter.Next();
    }
  }

  for (Key i = 0; i < R; i++) {
    TestOffsetSkipList::Iterator iter(&list);
    iter.SeekForPrev(i);
    std::set<Key>::iterator model_iter = keys.upper_bound(i);
    for (int j = 0; j < 3; j++) {
      if (model_iter == keys.begin()) {
        ASSERT_TRUE(!iter.Valid());
        break;
      }
      ASSERT_TRUE(iter.Valid());
      ASSERT_EQ(*--model_iter, Decode(iter.key()));
      iter.Prev();
    }
  }
  tok->release();
  list.TEST_Validate();
  ASSERT_GT(list.mem_size(), 0U);
  ASSERT_EQ(list.num_nodes(), keys.size());
}

TEST_F(OffsetSkipTest, InsertWithHint_Sequential) {
  const int N = 20000;
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  auto* tok = EnsureAcquired(&list);
  for (int i = 0; i < N; i++) {
    InsertWithHint(&list, static_cast<Key>(i), tok);
  }
  Validate(&list);
}

TEST_F(OffsetSkipTest, ConcurrentInsert) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  const int N = 4000;
  const int T = 4;
  std::vector<std::thread> threads;
  threads.reserve(T);
  for (int t = 0; t < T; t++) {
    threads.emplace_back([&list, t]() {
      auto* tok = EnsureAcquired(&list);
      for (int i = 0; i < N; i++) {
        Key key = static_cast<Key>(t) * N + i;
        char* buf = list.AllocateKey(sizeof(Key));
        ASSERT_NE(buf, nullptr);
        memcpy(buf, &key, sizeof(Key));
        ASSERT_EQ(list.InsertConcurrently(buf, tok), nullptr);
      }
      tok->idle();
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  {
    ScopedPin pin(&list);
    for (int t = 0; t < T; t++) {
      for (int i = 0; i < N; i++) {
        Key key = static_cast<Key>(t) * N + i;
        ASSERT_TRUE(list.Contains(key, pin.tok));
      }
    }
  }
  list.TEST_Validate();
  ASSERT_EQ(list.num_nodes(), static_cast<uint64_t>(T) * N);
}

TEST_F(OffsetSkipTest, InsertDuplicateFreesUnused) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  Key key = 42;
  char* first = list.AllocateKey(sizeof(Key));
  ASSERT_NE(first, nullptr);
  memcpy(first, &key, sizeof(Key));
  auto* tok = EnsureAcquired(&list);
  ASSERT_EQ(list.Insert(first, tok), nullptr);
  const size_t after_first = list.mem_size();
  char* dup = list.AllocateKey(sizeof(Key));
  ASSERT_NE(dup, nullptr);
  memcpy(dup, &key, sizeof(Key));
  ASSERT_EQ(list.Insert(dup, tok), first);
  list.FreeUnusedKey(dup, sizeof(Key));
  ASSERT_LE(list.mem_size(), after_first);
  {
    ScopedPin pin(&list);
    ASSERT_TRUE(list.Contains(key, pin.tok));
  }
  ASSERT_EQ(list.num_nodes(), 1U);
  list.TEST_Validate();
}

TEST_F(OffsetSkipTest, TokenLazyFreeRespectsReader) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  auto* writer = list.tls_token();
  auto* reader = new TestOffsetSkipList::Token();
  reader->acquire(&list);
  writer->acquire(&list);
  size_t pos = list.mempool().alloc(64);
  ASSERT_NE(pos, size_t(-1));
  list.LazyFree(pos, 64, writer);
  ASSERT_GT(list.lazy_free_bytes(), 0U);
  list.GC(writer);
  ASSERT_GT(list.lazy_free_bytes(), 0U);
  reader->idle();
  writer->idle();
  reader->release();
  writer->release();
  writer->acquire(&list);
  list.GC(writer);
  ASSERT_EQ(list.lazy_free_bytes(), 0U);
  writer->idle();
  reader->dispose();
}

TEST_F(OffsetSkipTest, TokenLazyFreeSoleWriter) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  auto* writer = list.tls_token();
  size_t pos = list.mempool().alloc(64);
  ASSERT_NE(pos, size_t(-1));
  writer->acquire(&list);
  list.LazyFree(pos, 64, writer);
  EXPECT_GT(list.lazy_free_bytes(), 0U);
  list.GC(writer);
  EXPECT_GT(list.lazy_free_bytes(), 0U);
  writer->release();
}

TEST_F(OffsetSkipTest, GCAllRespectsWatermark) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  auto* writer = list.tls_token();
  auto* reader = new TestOffsetSkipList::Token();
  size_t pos = list.mempool().alloc(64);
  ASSERT_NE(pos, size_t(-1));
  reader->acquire(&list);
  writer->acquire(&list);
  list.LazyFree(pos, 64, writer);
  writer->idle();
  EXPECT_GT(list.lazy_free_bytes(), 0U);
  list.GCAll();
  EXPECT_GT(list.lazy_free_bytes(), 0U);
  reader->release();
  list.GCAll();
  EXPECT_GT(list.lazy_free_bytes(), 0U);
  writer->release();
  reader->dispose();
}

TEST_F(OffsetSkipTest, TokenAcquireIdlePairs) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  auto* writer = list.tls_token();
  auto* reader = new TestOffsetSkipList::Token();
  for (int i = 0; i < 3; ++i) {
    writer->acquire(&list);
    writer->idle();
    reader->acquire(&list);
    reader->idle();
  }
  writer->acquire(&list);
  writer->release();
  reader->acquire(&list);
  reader->release();
  reader->dispose();
}

TEST_F(OffsetSkipTest, TokenGCRevokesAtMostEight) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  auto* writer = list.tls_token();
  auto* reader = new TestOffsetSkipList::Token();
  reader->acquire(&list);
  writer->acquire(&list);
  for (int i = 0; i < 10; ++i) {
    size_t pos = list.mempool().alloc(64);
    ASSERT_NE(pos, size_t(-1));
    list.LazyFree(pos, 64, writer);
  }
  ASSERT_EQ(list.lazy_free_bytes(), 640U);
  reader->idle();
  writer->idle();
  reader->release();
  writer->release();
  writer->acquire(&list);
  list.GC(writer);
  ASSERT_EQ(list.lazy_free_bytes(), 128U);
  list.GC(writer);
  ASSERT_EQ(list.lazy_free_bytes(), 0U);
  writer->idle();
  reader->dispose();
}

TEST_F(OffsetSkipTest, DupInsertKeepsValueLeading) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  Key key = 7;
  char* first = list.AllocateKey(sizeof(Key));
  ASSERT_NE(first, nullptr);
  memcpy(first, &key, sizeof(Key));
  auto* tok = EnsureAcquired(&list);
  ASSERT_EQ(list.Insert(first, tok), nullptr);

  constexpr size_t kLeading = 8;
  char* dup = list.AllocateKey(sizeof(Key), kLeading);
  ASSERT_NE(dup, nullptr);
  memcpy(dup, &key, sizeof(Key));
  const size_t vpos = list.KeyAllocPos(dup, kLeading);
  memset(const_cast<terark::byte_t*>(list.mem_data()) + vpos, 0xAB, kLeading);
  ASSERT_EQ(list.Insert(dup, tok), first);
  ASSERT_EQ(list.FreeUnusedKeyKeepLeading(dup, sizeof(Key), kLeading), vpos);
  for (size_t i = 0; i < kLeading; ++i) {
    ASSERT_EQ(list.mem_data()[vpos + i], 0xAB);
  }
  {
    ScopedPin pin(&list);
    ASSERT_TRUE(list.Contains(key, pin.tok));
  }
  list.TEST_Validate();
}

TEST_F(OffsetSkipTest, InsertWithHint_Reverse) {
  const int N = 4000;
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  auto* tok = EnsureAcquired(&list);
  for (int i = N - 1; i >= 0; --i) {
    ASSERT_TRUE(InsertWithHint(&list, static_cast<Key>(i), tok));
  }
  Validate(&list);
}

TEST_F(OffsetSkipTest, InsertWithHint_MultipleHints) {
  const int N = 3000;
  const int S = 12;
  Random rnd(534);
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  TestOffsetSkipList::Token* tokens[S];
  Key last_key[S];
  for (int i = 0; i < S; ++i) {
    tokens[i] = new TestOffsetSkipList::Token();
    EnsureAcquired(&list, tokens[i]);
    last_key[i] = 0;
  }
  for (int i = 0; i < N; ++i) {
    int s = static_cast<int>(rnd.Uniform(S));
    Key key = (static_cast<Key>(s) << 32) + (++last_key[s]);
    ASSERT_TRUE(InsertWithHint(&list, key, tokens[s]));
  }
  Validate(&list);
  for (int i = 0; i < S; ++i) {
    if (tokens[i]->state() == TestOffsetSkipList::AcquireDone ||
        tokens[i]->state() == TestOffsetSkipList::AcquireIdle) {
      tokens[i]->release();
    }
    tokens[i]->dispose();
  }
}

TEST_F(OffsetSkipTest, ConcurrentInsertWithHint) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  const int N = 800;
  const int T = 4;
  std::vector<std::thread> threads;
  threads.reserve(T);
  for (int t = 0; t < T; ++t) {
    threads.emplace_back([&list, t]() {
      auto* tok = EnsureAcquired(&list);
      for (int i = 0; i < N; ++i) {
        Key key = static_cast<Key>(t) * N + i;
        char* buf = list.AllocateKey(sizeof(Key));
        ASSERT_NE(buf, nullptr);
        memcpy(buf, &key, sizeof(Key));
        ASSERT_EQ(list.InsertWithHintConcurrently(buf, tok), nullptr);
      }
      tok->idle();
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  {
    ScopedPin pin(&list);
    for (int t = 0; t < T; ++t) {
      for (int i = 0; i < N; ++i) {
        Key key = static_cast<Key>(t) * N + i;
        ASSERT_TRUE(list.Contains(key, pin.tok));
      }
    }
  }
  list.TEST_Validate();
}

TEST_F(OffsetSkipTest, InsertWithHintAllocatesAndFinishHint) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  auto* tok = EnsureAcquired(&list);
  ASSERT_EQ(tok->m_splice_hint, nullptr);

  Key k1 = 10, k2 = 20, k3 = 30;
  char* n1 = list.AllocateKey(sizeof(Key));
  char* n2 = list.AllocateKey(sizeof(Key));
  char* n3 = list.AllocateKey(sizeof(Key));
  ASSERT_NE(n1, nullptr);
  ASSERT_NE(n2, nullptr);
  ASSERT_NE(n3, nullptr);
  memcpy(n1, &k1, sizeof(Key));
  memcpy(n2, &k2, sizeof(Key));
  memcpy(n3, &k3, sizeof(Key));
  ASSERT_EQ(list.InsertWithHint(n1, tok), nullptr);
  ASSERT_NE(tok->m_splice_hint, nullptr);
  auto* first = tok->m_splice_hint;
  ASSERT_EQ(list.InsertWithHint(n2, tok), nullptr);
  ASSERT_EQ(tok->m_splice_hint, first);

  list.FinishHint(tok);
  ASSERT_EQ(tok->m_splice_hint, first);
  ASSERT_EQ(tok->m_splice_hint->height, 0);
  ASSERT_EQ(list.InsertWithHint(n3, tok), nullptr);
  ASSERT_EQ(tok->m_splice_hint, first);
  list.FinishHint(tok);
  ASSERT_EQ(tok->m_splice_hint, first);
  ASSERT_EQ(tok->m_splice_hint->height, 0);
  {
    ScopedPin pin(&list);
    ASSERT_TRUE(list.Contains(k1, pin.tok));
    ASSERT_TRUE(list.Contains(k2, pin.tok));
    ASSERT_TRUE(list.Contains(k3, pin.tok));
  }
  list.TEST_Validate();
}

static char* AllocHeight1(TestOffsetSkipList* list, Key v) {
  char* buf = list->TEST_AllocateKeyWithHeight(sizeof(Key), 1);
  EXPECT_NE(buf, nullptr);
  memcpy(buf, &v, sizeof(Key));
  return buf;
}

static void InsertHeight1(TestOffsetSkipList* list, Key v) {
  char* buf = AllocHeight1(list, v);
  ASSERT_EQ(list->Insert(buf, EnsureAcquired(list)), nullptr);
}

static std::vector<Key> CollectOrder(TestOffsetSkipList* list) {
  std::vector<Key> out;
  TestOffsetSkipList::Iterator it(list);
  it.SeekToFirst();
  while (it.Valid()) {
    out.push_back(Decode(it.key()));
    it.Next();
  }
  return out;
}

// Stale prev loc > key. Old LinkFromSplice treated the loc as a stable
// Node* and linked after 30 (10,20,30,15). New recomputes from head.
TEST_F(OffsetSkipTest, StaleSplicePrevGreaterThanKey) {
  TestComparator cmp;
  {
    TestOffsetSkipList old_list(cmp, kTestMemCap);
    InsertHeight1(&old_list, 10);
    InsertHeight1(&old_list, 20);
    char* k30 = AllocHeight1(&old_list, 30);
    ASSERT_EQ(old_list.Insert(k30, EnsureAcquired(&old_list)), nullptr);
    char* k15 = AllocHeight1(&old_list, 15);
    auto* tok = EnsureAcquired(&old_list);
    old_list.TEST_AllocHint(tok);
    auto* splice = tok->m_splice_hint;
    splice->prev[0] = old_list.TEST_LocOf(k30);
    splice->next[0] = TestOffsetSkipList::nil;
    ASSERT_EQ(old_list.TEST_InsertSkipPrepareLegacy(k15, splice), nullptr);
    ASSERT_EQ(CollectOrder(&old_list), (std::vector<Key>{10, 20, 30, 15}));
  }
  {
    TestOffsetSkipList list(cmp, kTestMemCap);
    InsertHeight1(&list, 10);
    InsertHeight1(&list, 20);
    char* k30 = AllocHeight1(&list, 30);
    ASSERT_EQ(list.Insert(k30, EnsureAcquired(&list)), nullptr);
    char* k15 = AllocHeight1(&list, 15);
    auto* tok = list.tls_token();
    list.TEST_AllocHint(tok);
    auto* splice = tok->m_splice_hint;
    splice->prev[0] = list.TEST_LocOf(k30);
    splice->next[0] = TestOffsetSkipList::nil;
    ASSERT_EQ(list.TEST_InsertSkipPrepare(k15, splice), nullptr);
    Key v15 = 15;
    {
      ScopedPin pin(&list);
      ASSERT_TRUE(list.Contains(v15, pin.tok));
    }
    ASSERT_EQ(CollectOrder(&list), (std::vector<Key>{10, 15, 20, 30}));
    list.TEST_Validate();
  }
}

// Stale next loc < key. Old linked 10 -> 25 -> 20. New recomputes.
TEST_F(OffsetSkipTest, StaleSpliceNextLessThanKey) {
  TestComparator cmp;
  {
    TestOffsetSkipList old_list(cmp, kTestMemCap);
    char* k10 = AllocHeight1(&old_list, 10);
    char* k20 = AllocHeight1(&old_list, 20);
    ASSERT_EQ(old_list.Insert(k10, EnsureAcquired(&old_list)), nullptr);
    ASSERT_EQ(old_list.Insert(k20, EnsureAcquired(&old_list)), nullptr);
    InsertHeight1(&old_list, 30);
    char* k25 = AllocHeight1(&old_list, 25);
    auto* tok = EnsureAcquired(&old_list);
    old_list.TEST_AllocHint(tok);
    auto* splice = tok->m_splice_hint;
    splice->prev[0] = old_list.TEST_LocOf(k10);
    splice->next[0] = old_list.TEST_LocOf(k20);
    ASSERT_EQ(old_list.TEST_InsertSkipPrepareLegacy(k25, splice), nullptr);
    ASSERT_EQ(CollectOrder(&old_list), (std::vector<Key>{10, 25, 20, 30}));
  }
  {
    TestOffsetSkipList list(cmp, kTestMemCap);
    char* k10 = AllocHeight1(&list, 10);
    char* k20 = AllocHeight1(&list, 20);
    ASSERT_EQ(list.Insert(k10, EnsureAcquired(&list)), nullptr);
    ASSERT_EQ(list.Insert(k20, EnsureAcquired(&list)), nullptr);
    InsertHeight1(&list, 30);
    char* k25 = AllocHeight1(&list, 25);
    auto* tok = list.tls_token();
    list.TEST_AllocHint(tok);
    auto* splice = tok->m_splice_hint;
    splice->prev[0] = list.TEST_LocOf(k10);
    splice->next[0] = list.TEST_LocOf(k20);
    ASSERT_EQ(list.TEST_InsertSkipPrepare(k25, splice), nullptr);
    Key v25 = 25;
    {
      ScopedPin pin(&list);
      ASSERT_TRUE(list.Contains(v25, pin.tok));
    }
    ASSERT_EQ(CollectOrder(&list), (std::vector<Key>{10, 20, 25, 30}));
    list.TEST_Validate();
  }
}

// Old FindSpliceForLevel stopped on next == after even when after < key.
TEST_F(OffsetSkipTest, StaleAfterHintDoesNotStopEarly) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  char* nodes[4];
  const Key vals[] = {10, 20, 30, 40};
  for (int i = 0; i < 4; ++i) {
    nodes[i] = AllocHeight1(&list, vals[i]);
    ASSERT_EQ(list.Insert(nodes[i], EnsureAcquired(&list)), nullptr);
  }
  Key target = 35;
  TestOffsetSkipList::link_t old_prev = 0, old_next = 0;
  list.TEST_FindSpliceForLevelLegacy(Encode(&target), list.head_loc(),
                                     list.TEST_LocOf(nodes[1]), 0, &old_prev,
                                     &old_next);
  ASSERT_EQ(old_next, list.TEST_LocOf(nodes[1]));
  ASSERT_EQ(old_prev, list.TEST_LocOf(nodes[0]));

  TestOffsetSkipList::link_t out_prev = 0, out_next = 0;
  list.TEST_FindSpliceForLevel(Encode(&target), list.head_loc(),
                               list.TEST_LocOf(nodes[1]), 0, &out_prev,
                               &out_next);
  ASSERT_EQ(out_next, list.TEST_LocOf(nodes[3]));
  ASSERT_EQ(out_prev, list.TEST_LocOf(nodes[2]));
}

// Hint left prev == the already-inserted key. Old linked a duplicate;
// new equal(prev) rejects it.
TEST_F(OffsetSkipTest, StaleSplicePrevIsDuplicate) {
  TestComparator cmp;
  {
    TestOffsetSkipList old_list(cmp, kTestMemCap);
    char* first = AllocHeight1(&old_list, 10);
    ASSERT_EQ(old_list.Insert(first, EnsureAcquired(&old_list)), nullptr);
    char* dup = AllocHeight1(&old_list, 10);
    auto* tok = EnsureAcquired(&old_list);
    old_list.TEST_AllocHint(tok);
    auto* splice = tok->m_splice_hint;
    splice->prev[0] = old_list.TEST_LocOf(first);
    splice->next[0] = TestOffsetSkipList::nil;
    ASSERT_EQ(old_list.TEST_InsertSkipPrepareLegacy(dup, splice), nullptr);
    ASSERT_EQ(old_list.num_nodes(), 2U);
    ASSERT_EQ(CollectOrder(&old_list), (std::vector<Key>{10, 10}));
  }
  {
    TestOffsetSkipList list(cmp, kTestMemCap);
    char* first = AllocHeight1(&list, 10);
    ASSERT_EQ(list.Insert(first, EnsureAcquired(&list)), nullptr);
    char* dup = AllocHeight1(&list, 10);
    auto* tok = list.tls_token();
    list.TEST_AllocHint(tok);
    auto* splice = tok->m_splice_hint;
    splice->prev[0] = list.TEST_LocOf(first);
    splice->next[0] = TestOffsetSkipList::nil;
    ASSERT_EQ(list.TEST_InsertSkipPrepare(dup, splice), first);
    list.FreeUnusedKey(dup, sizeof(Key));
    ASSERT_EQ(list.num_nodes(), 1U);
    ASSERT_EQ(CollectOrder(&list), (std::vector<Key>{10}));
    list.TEST_Validate();
  }
}

// Concurrent same-key insert overwrites next_[0] before returning false.
// FreeUnusedKey must still see a restashed height or the pool is corrupted.
TEST_F(OffsetSkipTest, ConcurrentDuplicateFreesUnused) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  const int T = 4;
  const int M = 200;
  std::atomic<int> wins{0};
  std::vector<std::thread> threads;
  threads.reserve(T);
  for (int t = 0; t < T; ++t) {
    threads.emplace_back([&list, &wins, t]() {
      auto* tok = list.tls_token();
      std::vector<Key> order(M);
      std::iota(order.begin(), order.end(), Key{0});
      Random rnd(1000 + t);
      for (int i = M - 1; i > 0; --i) {
        std::swap(order[i], order[rnd.Uniform(i + 1)]);
      }
      tok->acquire(&list);
      for (Key key : order) {
        char* buf = list.AllocateKey(sizeof(Key));
        ASSERT_NE(buf, nullptr);
        memcpy(buf, &key, sizeof(Key));
        if (list.InsertConcurrently(buf, tok) == nullptr) {
          wins.fetch_add(1, std::memory_order_relaxed);
        } else {
          list.FreeUnusedKey(buf, sizeof(Key));
        }
      }
      tok->idle();
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  ASSERT_EQ(wins.load(), M);
  {
    ScopedPin pin(&list);
    for (int i = 0; i < M; ++i) {
      Key key = static_cast<Key>(i);
      ASSERT_TRUE(list.Contains(key, pin.tok));
    }
  }
  list.TEST_Validate();
  auto* tok = EnsureAcquired(&list);
  for (int i = 0; i < 32; ++i) {
    Key key = static_cast<Key>(M + i);
    char* buf = list.AllocateKey(sizeof(Key));
    ASSERT_NE(buf, nullptr);
    memcpy(buf, &key, sizeof(Key));
    ASSERT_EQ(list.Insert(buf, tok), nullptr);
    ASSERT_TRUE(list.Contains(key, tok));
  }
  list.TEST_Validate();
}

TEST_F(OffsetSkipTest, SeekMissingEstimateCountRandomSeek) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  const Key keys[] = {10, 20, 30, 40, 50};
  for (Key key : keys) {
    Insert(&list, key);
  }
  TestOffsetSkipList::Iterator iter(&list);
  Key t = 25;
  iter.Seek(t);
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(Decode(iter.key()), 30U);
  iter.SeekForPrev(t);
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(Decode(iter.key()), 20U);
  t = 5;
  iter.Seek(t);
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(Decode(iter.key()), 10U);
  iter.SeekForPrev(t);
  ASSERT_FALSE(iter.Valid());
  t = 60;
  iter.Seek(t);
  ASSERT_FALSE(iter.Valid());
  iter.SeekForPrev(t);
  ASSERT_TRUE(iter.Valid());
  ASSERT_EQ(Decode(iter.key()), 50U);

  Key lo = 10;
  Key mid = 30;
  Key hi = 51;
  ASSERT_LE(list.EstimateCount(lo, &iter),
            list.EstimateCount(mid, &iter));
  ASSERT_LE(list.EstimateCount(mid, &iter),
            list.EstimateCount(hi, &iter));

  std::set<Key> present(std::begin(keys), std::end(keys));
  for (int i = 0; i < 16; ++i) {
    iter.RandomSeek();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(present.count(Decode(iter.key())), 1U);
  }
  list.TEST_Validate();
}

TEST_F(OffsetSkipTest, TokenReadonlySkipsQueue) {
  TestComparator cmp;
  TestOffsetSkipList list(cmp, kTestMemCap);
  Insert(&list, 10);
  Insert(&list, 20);
  auto* tok = EnsureAcquired(&list);
  ASSERT_EQ(tok->state(), TestOffsetSkipList::AcquireDone);
  ASSERT_GT(list.token_qlen(), 0U);
  list.set_readonly();
  ASSERT_TRUE(list.is_readonly());
  ASSERT_GT(list.token_qlen(), 0U);
  tok->idle();
  ASSERT_EQ(list.token_qlen(), 0U);
  ASSERT_EQ(tok->state(), TestOffsetSkipList::AcquireIdle);
  tok->acquire(&list);
  ASSERT_EQ(list.token_qlen(), 0U);
  ASSERT_EQ(tok->state(), TestOffsetSkipList::AcquireDone);
  Key k10 = 10;
  ASSERT_TRUE(list.Contains(k10, tok));
  ASSERT_NE(list.Get(k10, tok), nullptr);
  tok->release();
  ASSERT_EQ(list.token_qlen(), 0U);
  ASSERT_EQ(tok->state(), TestOffsetSkipList::ReleaseDone);
  {
    TestOffsetSkipList::ReadonlyIterator iter(&list);
    ASSERT_EQ(list.token_qlen(), 0U);
    iter.SeekToFirst();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(Decode(iter.key()), 10U);
    iter.Next();
    ASSERT_TRUE(iter.Valid());
    ASSERT_EQ(Decode(iter.key()), 20U);
  }
  ASSERT_EQ(list.token_qlen(), 0U);
}

TEST_F(OffsetSkipTest, AttachReadonlySeesKeys) {
  TestComparator cmp;
  TestOffsetSkipList src(cmp, kTestMemCap);
  Insert(&src, 10);
  Insert(&src, 20);
  const size_t prefix =
      sizeof(TestOffsetSkipList::link_t) * size_t(src.max_height() - 1);
  ASSERT_GE(size_t(src.head_loc()) * src.mem_align_size(), prefix);
  TestOffsetSkipList attached(
      cmp,
      terark::fstring(reinterpret_cast<const char*>(src.mem_data()),
                      src.mem_size()),
      src.head_loc(), src.max_height(), src.k_branching(), src.num_nodes());
  ASSERT_TRUE(attached.is_readonly());
  ASSERT_EQ(attached.num_nodes(), src.num_nodes());
  static_assert(std::is_base_of<TestOffsetSkipList::Token,
                                TestOffsetSkipList::Iterator>::value,
                "");
  static_assert(!std::is_base_of<TestOffsetSkipList::Token,
                                 TestOffsetSkipList::ReadonlyIterator>::value,
                "");
  ASSERT_LT(sizeof(TestOffsetSkipList::ReadonlyIterator),
            sizeof(TestOffsetSkipList::Iterator));
  ASSERT_EQ(attached.token_qlen(), 0U);
  TestOffsetSkipList::ReadonlyIterator it(&attached);
  ASSERT_EQ(attached.token_qlen(), 0U);
  it.SeekToFirst();
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ(Decode(it.key()), 10U);
  it.Next();
  ASSERT_TRUE(it.Valid());
  ASSERT_EQ(Decode(it.key()), 20U);
  it.Next();
  ASSERT_FALSE(it.Valid());
}

TEST_F(OffsetSkipTest, AttachHeadLocBelowPrefixDies) {
  TestComparator cmp;
  char buf[256] = {};
  terark::fstring mem(buf, sizeof(buf));
  ASSERT_DEATH(
      { TestOffsetSkipList bad(cmp, mem, 0, 14, 4, 0); },
      "");
}

std::shared_ptr<MemTableRepFactory> EasyNewMemTableRep(Slice class_name,
                                                       Slice params);
TableFactory* EasyNewTableFactory(Slice class_name, Slice params);

static MemTable* NewOffsetMemTable(Options* options, WriteBufferManager* wb,
                                   const std::string& js) {
  options->memtable_factory = EasyNewMemTableRep("OffsetSkipList", js);
  if (options->cf_paths.empty()) {
    options->cf_paths.emplace_back(test::TmpDir(), 0);
  }
  InternalKeyComparator cmp(options->comparator);
  ImmutableOptions ioptions(*options);
  return new MemTable(cmp, ioptions, MutableCFOptions(*options), wb,
                      kMaxSequenceNumber, 0);
}

static bool MemGet(MemTable* mem, const Slice& ukey, SequenceNumber snap,
                   std::string* value, Status* st = nullptr) {
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;
  Status s;
  PinnableSlice v;
  bool found =
      mem->Get(LookupKey(ukey, snap), &v, nullptr, nullptr, &s, &merge_context,
               &max_covering_tombstone_seq, ReadOptions(), false);
  if (st) {
    *st = s;
  }
  if (found && s.ok() && value) {
    *value = v.ToString();
  }
  return found;
}

static size_t AppendWalKv(std::string* payload, const std::string& k,
                          const std::string& v) {
  PutVarint32(payload, uint32_t(k.size()));
  payload->append(k);
  PutVarint32(payload, uint32_t(v.size()));
  const size_t val_pos = payload->size();
  payload->append(v);
  return val_pos;
}

static void AddLogRef(MemTable* mem, SequenceNumber seq, const Slice& ukey,
                      size_t val_pos, const Slice& val, ReadonlyFileMmap* wal,
                      uint64_t fileno = 1, bool concurrent = false) {
  KeyValuePassMemTable kv;
  kv.value = val;
  kv.val_pos = val_pos;
  kv.key_len = uint32_t(ukey.size());
  kv.fileno = fileno;
  kv.wal_file = wal;
  MemTablePostProcessInfo post;
  ASSERT_OK(mem->Add(seq, kTypeValue, ukey,
                     Slice(reinterpret_cast<const char*>(&kv), sizeof(kv)),
                     nullptr, concurrent, concurrent ? &post : nullptr));
}

TEST(OffsetSkipRepTest, Factory) {
  auto fac = EasyNewMemTableRep("OffsetSkipList", "{}");
  ASSERT_NE(fac, nullptr);
  ASSERT_STREQ(fac->Name(), "OffsetSkipList");
  ASSERT_TRUE(fac->IsInsertConcurrentlySupported());
  ASSERT_TRUE(fac->CanHandleDuplicatedKey());
  std::string opt = fac->GetPrintableOptions();
  ASSERT_NE(opt.find("token_use_idle"), std::string::npos);
}

TEST(OffsetSkipRepTest, EasyNewGeneric) {
  auto empty = EasyNewMemTableRep("OffsetSkipList", "");
  ASSERT_NE(empty, nullptr);
  ASSERT_STREQ(empty->Name(), "OffsetSkipList");

#ifdef HAS_TOPLING_CSPP_MEMTABLE
  auto cspp = EasyNewMemTableRep("cspp", R"({"mem_cap":1048576})");
  ASSERT_NE(cspp, nullptr);
  ASSERT_STREQ(cspp->Name(), "CSPPMemTabFactory");
#endif

  try {
    (void)EasyNewMemTableRep("NoSuchMemTableRep", "{}");
    FAIL();
  } catch (const Status& s) {
    ASSERT_TRUE(s.IsNotFound());
  }
}

TEST(OffsetSkipRepTest, FactoryTokenOptsReleasePark) {
  Options options;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(NewOffsetMemTable(
      &options, &wb, R"({"mem_cap":16777216,"token_use_idle":false})"));
  ASSERT_OK(mem->Add(1, kTypeValue, "k", "v", nullptr));
  std::string value;
  ASSERT_TRUE(MemGet(mem.get(), "k", 100, &value));
  ASSERT_EQ(value, "v");
  ASSERT_OK(mem->Add(2, kTypeValue, "k", "v2", nullptr));
  ASSERT_TRUE(MemGet(mem.get(), "k", 100, &value));
  ASSERT_EQ(value, "v2");
}

TEST(OffsetSkipRepTest, InsertGetIterate) {
  Options options;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  SequenceNumber seq = 1;
  ASSERT_OK(mem->Add(seq, kTypeValue, "key1", "v1", nullptr));
  ASSERT_OK(mem->Add(seq + 1, kTypeValue, "key2", "v2", nullptr));
  ASSERT_OK(mem->Add(seq + 2, kTypeValue, "key3", "v3", nullptr));

  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;
  Status s;
  PinnableSlice value;
  bool found = mem->Get(LookupKey("key2", seq + 10), &value, nullptr, nullptr,
                        &s, &merge_context, &max_covering_tombstone_seq,
                        ReadOptions(), false);
  ASSERT_TRUE(found);
  ASSERT_OK(s);
  ASSERT_EQ(value.ToString(), "v2");
  value.Reset();

  found = mem->Get(LookupKey("missing", seq + 10), &value, nullptr, nullptr, &s,
                   &merge_context, &max_covering_tombstone_seq, ReadOptions(),
                   false);
  ASSERT_FALSE(found);

  Arena arena;
  InternalIterator* it = mem->NewIterator(ReadOptions(), &arena);
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key1");
  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key2");
  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key3");
  it->Next();
  ASSERT_FALSE(it->Valid());
  it->~InternalIterator();

  ASSERT_GT(mem->ApproximateMemoryUsage(), 0U);
}

TEST(OffsetSkipRepTest, DuplicateSeq) {
  Options options;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  SequenceNumber seq = 100;
  ASSERT_OK(mem->Add(seq, kTypeValue, "key", "v", nullptr));
  ASSERT_TRUE(mem->Add(seq, kTypeValue, "key", "v", nullptr).IsTryAgain());
  ASSERT_OK(mem->Add(seq + 1, kTypeValue, "key", "v2", nullptr));
}

TEST(OffsetSkipRepTest, InsertWithHintAndLookahead) {
  Options options;
  options.memtable_insert_with_hint_prefix_extractor.reset(
      NewFixedPrefixTransform(3));
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(NewOffsetMemTable(
      &options, &wb, R"({"lookahead":8,"mem_cap":16777216})"));
  SequenceNumber seq = 1;
  for (int i = 0; i < 50; ++i) {
    ASSERT_OK(
        mem->Add(seq++, kTypeValue, "pre" + std::to_string(i), "v", nullptr));
  }
  Arena arena;
  InternalIterator* it = mem->NewIterator(ReadOptions(), &arena);
  it->Seek(InternalKey("pre10", 100, kTypeValue).Encode());
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "pre10");
  it->~InternalIterator();
}

TEST(OffsetSkipRepTest, PrefixHintGetTwoPrefixesAndVersions) {
  Options options;
  options.memtable_insert_with_hint_prefix_extractor.reset(
      NewFixedPrefixTransform(3));
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  SequenceNumber seq = 1;
  for (int i = 0; i < 8; ++i) {
    ASSERT_OK(
        mem->Add(seq++, kTypeValue, "aaa", "va" + std::to_string(i), nullptr));
    ASSERT_OK(
        mem->Add(seq++, kTypeValue, "bbb", "vb" + std::to_string(i), nullptr));
  }
  std::string value;
  ASSERT_TRUE(MemGet(mem.get(), "aaa", 100, &value));
  ASSERT_EQ(value, "va7");
  ASSERT_TRUE(MemGet(mem.get(), "bbb", 100, &value));
  ASSERT_EQ(value, "vb7");
  ASSERT_TRUE(MemGet(mem.get(), "aaa", 2, &value));
  ASSERT_EQ(value, "va0");
}

TEST(OffsetSkipRepTest, FinishHintParksWriterToken) {
  Options options;
  options.allow_concurrent_memtable_write = true;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  void* hint = nullptr;
  MemTablePostProcessInfo post;
  ASSERT_OK(mem->Add(1, kTypeValue, "k1", "v1", nullptr, true, &post, &hint));
  ASSERT_NE(hint, nullptr);
  void* first = hint;
  ASSERT_TRUE(mem->Add(1, kTypeValue, "k1", "v1", nullptr, true, &post, &hint)
                  .IsTryAgain());
  ASSERT_EQ(hint, first);
  ASSERT_OK(mem->Add(2, kTypeValue, "k2", "v2", nullptr, true, &post, &hint));
  ASSERT_EQ(hint, first);
  mem->FinishHint(hint);
  hint = nullptr;
  ASSERT_OK(mem->Add(3, kTypeValue, "k3", "v3", nullptr, true, &post, &hint));
  ASSERT_EQ(hint, first);
  mem->FinishHint(hint);
  std::string value;
  ASSERT_TRUE(MemGet(mem.get(), "k1", 100, &value));
  ASSERT_EQ(value, "v1");
  ASSERT_TRUE(MemGet(mem.get(), "k2", 100, &value));
  ASSERT_EQ(value, "v2");
  ASSERT_TRUE(MemGet(mem.get(), "k3", 100, &value));
  ASSERT_EQ(value, "v3");
}

TEST(OffsetSkipRepTest, ConcurrentHintPerThread) {
  Options options;
  options.allow_concurrent_memtable_write = true;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  const int T = 4;
  const int N = 20;
  std::vector<std::thread> threads;
  threads.reserve(T);
  for (int t = 0; t < T; ++t) {
    threads.emplace_back([&mem, t]() {
      void* hint = nullptr;
      MemTablePostProcessInfo post;
      for (int i = 0; i < N; ++i) {
        SequenceNumber seq = static_cast<SequenceNumber>(t) * N + i + 1;
        std::string key = "t" + std::to_string(t) + "k" + std::to_string(i);
        ASSERT_OK(
            mem->Add(seq, kTypeValue, key, "v", nullptr, true, &post, &hint));
        if (i == 0) {
          ASSERT_NE(hint, nullptr);
        }
      }
      mem->FinishHint(hint);
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  std::string value;
  for (int t = 0; t < T; ++t) {
    for (int i = 0; i < N; ++i) {
      std::string key = "t" + std::to_string(t) + "k" + std::to_string(i);
      ASSERT_TRUE(MemGet(mem.get(), key, 10000, &value));
      ASSERT_EQ(value, "v");
    }
  }
}

TEST(OffsetSkipRepTest, MultiVersion) {
  Options options;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  ASSERT_OK(mem->Add(1, kTypeValue, "k", "v1", nullptr));
  ASSERT_OK(mem->Add(3, kTypeValue, "k", "v3", nullptr));
  ASSERT_OK(mem->Add(2, kTypeValue, "k", "v2", nullptr));
  ASSERT_OK(mem->Add(4, kTypeValue, "z", "vz", nullptr));

  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;
  Status s;
  PinnableSlice value;
  bool found = mem->Get(LookupKey("k", 100), &value, nullptr, nullptr, &s,
                        &merge_context, &max_covering_tombstone_seq,
                        ReadOptions(), false);
  ASSERT_TRUE(found);
  ASSERT_OK(s);
  ASSERT_EQ(value.ToString(), "v3");
  value.Reset();

  found =
      mem->Get(LookupKey("k", 2), &value, nullptr, nullptr, &s, &merge_context,
               &max_covering_tombstone_seq, ReadOptions(), false);
  ASSERT_TRUE(found);
  ASSERT_OK(s);
  ASSERT_EQ(value.ToString(), "v2");

  Arena arena;
  InternalIterator* it = mem->NewIterator(ReadOptions(), &arena);
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "k");
  ASSERT_EQ(it->value().ToString(), "v3");
  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "k");
  ASSERT_EQ(it->value().ToString(), "v2");
  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "k");
  ASSERT_EQ(it->value().ToString(), "v1");
  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "z");
  ASSERT_EQ(it->value().ToString(), "vz");
  it->Next();
  ASSERT_FALSE(it->Valid());
  it->~InternalIterator();
}

TEST(OffsetSkipRepTest, ConcurrentSameUserKey) {
  Options options;
  options.allow_concurrent_memtable_write = true;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  const int T = 4;
  const int N = 50;
  std::vector<std::thread> threads;
  threads.reserve(T);
  for (int t = 0; t < T; ++t) {
    threads.emplace_back([&mem, t]() {
      MemTablePostProcessInfo post;
      for (int i = 0; i < N; ++i) {
        SequenceNumber seq = static_cast<SequenceNumber>(t) * N + i + 1;
        ASSERT_OK(mem->Add(seq, kTypeValue, "shared", "v" + std::to_string(seq),
                           nullptr, true, &post));
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;
  Status s;
  PinnableSlice value;
  bool found = mem->Get(LookupKey("shared", 100000), &value, nullptr, nullptr,
                        &s, &merge_context, &max_covering_tombstone_seq,
                        ReadOptions(), false);
  ASSERT_TRUE(found);
  ASSERT_OK(s);
  ASSERT_EQ(value.ToString(), "v" + std::to_string(T * N));
}

TEST(OffsetSkipRepTest, ConcurrentInsert) {
  Options options;
  options.allow_concurrent_memtable_write = true;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  const int N = 200;
  const int T = 4;
  std::vector<std::thread> threads;
  threads.reserve(T);
  for (int t = 0; t < T; ++t) {
    threads.emplace_back([&mem, t]() {
      MemTablePostProcessInfo post;
      for (int i = 0; i < N; ++i) {
        SequenceNumber seq = static_cast<SequenceNumber>(t) * N + i + 1;
        std::string key = "k" + std::to_string(seq);
        ASSERT_OK(mem->Add(seq, kTypeValue, key, "v", nullptr, true, &post));
      }
    });
  }
  for (auto& th : threads) {
    th.join();
  }
  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;
  Status s;
  for (int t = 0; t < T; ++t) {
    for (int i = 0; i < N; ++i) {
      SequenceNumber seq = static_cast<SequenceNumber>(t) * N + i + 1;
      std::string key = "k" + std::to_string(seq);
      PinnableSlice value;
      bool found = mem->Get(LookupKey(key, 100000), &value, nullptr, nullptr,
                            &s, &merge_context, &max_covering_tombstone_seq,
                            ReadOptions(), false);
      ASSERT_TRUE(found);
      ASSERT_OK(s);
      ASSERT_EQ(value.ToString(), "v");
    }
  }
}

TEST(OffsetSkipRepTest, IterateSeekPrevLast) {
  Options options;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  ASSERT_OK(mem->Add(1, kTypeValue, "key1", "v1", nullptr));
  ASSERT_OK(mem->Add(2, kTypeValue, "key2", "v2", nullptr));
  ASSERT_OK(mem->Add(3, kTypeValue, "key3", "v3", nullptr));

  Arena arena;
  InternalIterator* it = mem->NewIterator(ReadOptions(), &arena);
  it->SeekToLast();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key3");
  ASSERT_EQ(it->value().ToString(), "v3");
  it->Prev();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key2");
  it->Prev();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key1");
  it->Prev();
  ASSERT_FALSE(it->Valid());

  it->Seek(InternalKey("key2a", 100, kTypeValue).Encode());
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key3");
  it->SeekForPrev(InternalKey("key2a", 100, kTypeValue).Encode());
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key2");
  it->~InternalIterator();
}

TEST(OffsetSkipRepTest, DeleteAndEmptyValue) {
  Options options;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  ASSERT_OK(mem->Add(1, kTypeValue, "k", "", nullptr));
  std::string value;
  Status st;
  ASSERT_TRUE(MemGet(mem.get(), "k", 1, &value, &st));
  ASSERT_OK(st);
  ASSERT_EQ(value, "");

  ASSERT_OK(mem->Add(2, kTypeDeletion, "k", "", nullptr));
  ASSERT_TRUE(MemGet(mem.get(), "k", 100, &value, &st));
  ASSERT_TRUE(st.IsNotFound());
  ASSERT_TRUE(MemGet(mem.get(), "k", 1, &value, &st));
  ASSERT_OK(st);
  ASSERT_EQ(value, "");
}

TEST(OffsetSkipRepTest, ManyVersionsGrowCap) {
  Options options;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  for (int i = 1; i <= 32; i += 2) {
    ASSERT_OK(mem->Add(i, kTypeValue, "k", "v" + std::to_string(i), nullptr));
  }
  for (int i = 2; i <= 32; i += 2) {
    ASSERT_OK(mem->Add(i, kTypeValue, "k", "v" + std::to_string(i), nullptr));
  }
  std::string value;
  ASSERT_TRUE(MemGet(mem.get(), "k", 100, &value));
  ASSERT_EQ(value, "v32");
  ASSERT_TRUE(MemGet(mem.get(), "k", 7, &value));
  ASSERT_EQ(value, "v7");

  Arena arena;
  InternalIterator* it = mem->NewIterator(ReadOptions(), &arena);
  it->SeekToFirst();
  int n = 0;
  while (it->Valid()) {
    ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "k");
    ++n;
    it->Next();
  }
  ASSERT_EQ(n, 32);
  it->~InternalIterator();
}

TEST(OffsetSkipRepTest, UnalignedUserKey) {
  Options options;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  ASSERT_OK(mem->Add(1, kTypeValue, "x", "1", nullptr));
  ASSERT_OK(mem->Add(2, kTypeValue, "xy", "2", nullptr));
  ASSERT_OK(mem->Add(3, kTypeValue, "xyz", "3", nullptr));
  ASSERT_OK(mem->Add(4, kTypeValue, "xyzz", "4", nullptr));
  std::string value;
  ASSERT_TRUE(MemGet(mem.get(), "x", 100, &value));
  ASSERT_EQ(value, "1");
  ASSERT_TRUE(MemGet(mem.get(), "xy", 100, &value));
  ASSERT_EQ(value, "2");
  ASSERT_TRUE(MemGet(mem.get(), "xyz", 100, &value));
  ASSERT_EQ(value, "3");
  ASSERT_TRUE(MemGet(mem.get(), "xyzz", 100, &value));
  ASSERT_EQ(value, "4");

  Arena arena;
  InternalIterator* it = mem->NewIterator(ReadOptions(), &arena);
  it->SeekToFirst();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "x");
  it->Next();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "xy");
  it->Next();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "xyz");
  it->Next();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "xyzz");
  it->Next();
  ASSERT_FALSE(it->Valid());
  it->~InternalIterator();
}

class LongerKeyFirstComparator : public Comparator {
 public:
  const char* Name() const override { return "LongerKeyFirstComparator"; }
  int Compare(const Slice& a, const Slice& b) const override {
    if (a.size() != b.size()) {
      return a.size() > b.size() ? -1 : 1;
    }
    return a.compare(b);
  }
  void FindShortestSeparator(std::string*, const Slice&) const override {}
  void FindShortSuccessor(std::string*) const override {}
};

TEST(OffsetSkipRepTest, ReverseBytewiseOrder) {
  Options options;
  options.comparator = ReverseBytewiseComparator();
  options.memtable_insert_with_hint_prefix_extractor.reset(
      NewFixedPrefixTransform(1));
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(NewOffsetMemTable(
      &options, &wb, R"({"lookahead":8,"mem_cap":16777216})"));
  ASSERT_OK(mem->Add(1, kTypeValue, "a", "va", nullptr));
  ASSERT_OK(mem->Add(2, kTypeValue, "b", "vb", nullptr));
  ASSERT_OK(mem->Add(3, kTypeValue, "c", "vc", nullptr));
  Arena arena;
  InternalIterator* it = mem->NewIterator(ReadOptions(), &arena);
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "c");
  it->Next();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "b");
  it->Next();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "a");
  it->Next();
  ASSERT_FALSE(it->Valid());

  it->Seek(InternalKey("b", 100, kTypeValue).Encode());
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "b");
  it->Seek(InternalKey("bb", 100, kTypeValue).Encode());
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "b");
  it->SeekForPrev(InternalKey("bb", 100, kTypeValue).Encode());
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "c");
  it->Seek(InternalKey("d", 100, kTypeValue).Encode());
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "c");
  it->Seek(InternalKey("\x01", 100, kTypeValue).Encode());
  ASSERT_FALSE(it->Valid());
  it->SeekForPrev(InternalKey("\x01", 100, kTypeValue).Encode());
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "a");
  it->SeekToLast();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "a");
  it->Prev();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "b");
  it->Prev();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "c");
  it->Prev();
  ASSERT_FALSE(it->Valid());
  it->~InternalIterator();
  std::string value;
  ASSERT_TRUE(MemGet(mem.get(), "b", 100, &value));
  ASSERT_EQ(value, "vb");
}

TEST(OffsetSkipRepTest, FallbackUserKeyOrder) {
  LongerKeyFirstComparator cmp;
  ASSERT_FALSE(cmp.IsBytewise());
  Options options;
  options.comparator = &cmp;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  ASSERT_OK(mem->Add(1, kTypeValue, "a", "va", nullptr));
  ASSERT_OK(mem->Add(2, kTypeValue, "bb", "vb", nullptr));
  ASSERT_OK(mem->Add(3, kTypeValue, "ccc", "vc", nullptr));
  Arena arena;
  InternalIterator* it = mem->NewIterator(ReadOptions(), &arena);
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "ccc");
  it->Next();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "bb");
  it->Next();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "a");
  it->Next();
  ASSERT_FALSE(it->Valid());
  it->Seek(InternalKey("bb", 100, kTypeValue).Encode());
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "bb");
  // "cc" sits between "bb" and "a" under longer-first order.
  it->Seek(InternalKey("cc", 100, kTypeValue).Encode());
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "a");
  it->SeekForPrev(InternalKey("cc", 100, kTypeValue).Encode());
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "bb");
  it->~InternalIterator();
  std::string value;
  ASSERT_TRUE(MemGet(mem.get(), "bb", 100, &value));
  ASSERT_EQ(value, "vb");
}

TEST(OffsetSkipRepTest, ConcurrentReadDuringWrite) {
  Options options;
  options.allow_concurrent_memtable_write = true;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  ASSERT_OK(mem->Add(1, kTypeValue, "k", "v1", nullptr));

  std::atomic<bool> done{false};
  std::vector<std::thread> readers;
  readers.reserve(3);
  for (int t = 0; t < 3; ++t) {
    readers.emplace_back([&]() {
      while (!done.load(std::memory_order_relaxed)) {
        std::string value;
        Status st;
        if (MemGet(mem.get(), "k", 100000, &value, &st) && st.ok()) {
          ASSERT_FALSE(value.empty());
          ASSERT_EQ(value[0], 'v');
        }
        Arena arena;
        InternalIterator* it = mem->NewIterator(ReadOptions(), &arena);
        it->SeekToFirst();
        if (it->Valid()) {
          ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "k");
        }
        it->~InternalIterator();
      }
    });
  }
  for (int i = 2; i <= 64; ++i) {
    ASSERT_OK(mem->Add(i, kTypeValue, "k", "v" + std::to_string(i), nullptr));
  }
  done.store(true, std::memory_order_relaxed);
  for (auto& th : readers) {
    th.join();
  }
  std::string value;
  ASSERT_TRUE(MemGet(mem.get(), "k", 100000, &value));
  ASSERT_EQ(value, "v64");
}

// Same user key, concurrent writers (seqs interleave). Readers check each
// visible internal key's seq against its value — the old in-place memmove
// could pair a new tag with a shifted payload.
TEST(OffsetSkipRepTest, ConcurrentOutOfOrderReadDuringWrite) {
  Options options;
  options.allow_concurrent_memtable_write = true;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(
      NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
  const int T = 4;
  const int N = 40;
  std::atomic<bool> done{false};
  std::vector<std::thread> readers;
  readers.reserve(3);
  for (int t = 0; t < 3; ++t) {
    readers.emplace_back([&]() {
      while (!done.load(std::memory_order_relaxed)) {
        std::string value;
        Status st;
        if (MemGet(mem.get(), "k", 100000, &value, &st) && st.ok()) {
          ASSERT_GE(value.size(), 2U);
          ASSERT_EQ(value[0], 'v');
        }
        Arena arena;
        InternalIterator* it = mem->NewIterator(ReadOptions(), &arena);
        for (it->SeekToFirst(); it->Valid(); it->Next()) {
          ParsedInternalKey ikey;
          ASSERT_OK(ParseInternalKey(it->key(), &ikey, false /*log_err_key*/));
          ASSERT_EQ(ikey.user_key.ToString(), "k");
          ASSERT_EQ(it->value().ToString(),
                    "v" + std::to_string(ikey.sequence));
        }
        it->~InternalIterator();
      }
    });
  }
  std::vector<std::thread> writers;
  writers.reserve(T);
  for (int t = 0; t < T; ++t) {
    writers.emplace_back([&mem, t]() {
      MemTablePostProcessInfo post;
      for (int i = 0; i < N; ++i) {
        SequenceNumber seq = static_cast<SequenceNumber>(t) * N + i + 1;
        ASSERT_OK(mem->Add(seq, kTypeValue, "k", "v" + std::to_string(seq),
                           nullptr, true, &post));
      }
    });
  }
  for (auto& th : writers) {
    th.join();
  }
  done.store(true, std::memory_order_relaxed);
  for (auto& th : readers) {
    th.join();
  }
  std::string value;
  ASSERT_TRUE(MemGet(mem.get(), "k", 100000, &value));
  ASSERT_EQ(value, "v" + std::to_string(T * N));
  Arena arena;
  InternalIterator* it = mem->NewIterator(ReadOptions(), &arena);
  int seen = 0;
  for (it->SeekToFirst(); it->Valid(); it->Next()) {
    ParsedInternalKey ikey;
    ASSERT_OK(ParseInternalKey(it->key(), &ikey, false /*log_err_key*/));
    ASSERT_EQ(ikey.user_key.ToString(), "k");
    ASSERT_EQ(it->value().ToString(), "v" + std::to_string(ikey.sequence));
    ++seen;
  }
  it->~InternalIterator();
  ASSERT_EQ(seen, T * N);
}

TEST(OffsetSkipRepTest, ContainsAndApprox) {
  InternalKeyComparator icmp(BytewiseComparator());
  MemTable::KeyComparator cmp(icmp);
  Arena arena;
  auto fac = EasyNewMemTableRep("OffsetSkipList", R"({"mem_cap":16777216})");
  std::unique_ptr<MemTableRep> rep(
      fac->CreateMemTableRep(cmp, &arena, nullptr, nullptr));
  ASSERT_FALSE(rep->NeedsUserKeyCompareInGet());
  ASSERT_TRUE(
      rep->InsertKeyValue(PackSequenceAndType(1, kTypeValue), "a", "va"));
  ASSERT_TRUE(
      rep->InsertKeyValue(PackSequenceAndType(2, kTypeValue), "m", "vm"));
  ASSERT_TRUE(
      rep->InsertKeyValue(PackSequenceAndType(3, kTypeValue), "z", "vz"));
  InternalKey ia("a", 1, kTypeValue);
  InternalKey missing("b", 1, kTypeValue);
  ASSERT_TRUE(rep->Contains(ia.Encode()));
  ASSERT_FALSE(rep->Contains(missing.Encode()));
  InternalKey start("a", kMaxSequenceNumber, kValueTypeForSeek);
  InternalKey end("z", 0, kTypeValue);
  (void)rep->ApproximateNumEntries(start.Encode(), end.Encode());
}

TEST(OffsetSkipRepTest, SupportFlags) {
  Options options;
  WriteBufferManager wb(options.db_write_buffer_size);
  {
    std::unique_ptr<MemTable> mem(
        NewOffsetMemTable(&options, &wb, R"({"mem_cap":16777216})"));
    ASSERT_FALSE(mem->SupportConvertToSST());
  }
  {
    std::unique_ptr<MemTable> mem(NewOffsetMemTable(
        &options, &wb, R"({"mem_cap":16777216,"convert_to_sst":"kDumpMem"})"));
    ASSERT_TRUE(mem->SupportConvertToSST());
  }

  InternalKeyComparator icmp(BytewiseComparator());
  MemTable::KeyComparator cmp(icmp);
  Arena arena;
  {
    auto fac = EasyNewMemTableRep("OffsetSkipList", "{}");
    std::unique_ptr<MemTableRep> rep(
        fac->CreateMemTableRep(cmp, &arena, nullptr, nullptr));
    ASSERT_FALSE(rep->SupportMemTableAsLogIndex());
    ASSERT_FALSE(rep->SupportConvertToSST());
    rep->InitSetMemTableAsLogIndex(true);
    ASSERT_TRUE(rep->SupportMemTableAsLogIndex());
    rep->InitSetMemTableAsLogIndex(false);
    ASSERT_FALSE(rep->SupportMemTableAsLogIndex());
  }
  {
    auto fac = EasyNewMemTableRep("OffsetSkipList",
                                  R"({"convert_to_sst":"kDumpMem"})");
    std::unique_ptr<MemTableRep> rep(
        fac->CreateMemTableRep(cmp, &arena, nullptr, nullptr));
    ASSERT_TRUE(rep->SupportConvertToSST());
  }
  {
    std::unique_ptr<TableFactory> tf(
        EasyNewTableFactory("OffsetSkipListTable", "{}"));
    ASSERT_FALSE(tf->IsDeleteRangeSupported());
  }
}

TEST(OffsetSkipRepTest, ConvertToSST_DumpMem) {
  std::string dir = test::PerThreadDBPath("offset_skiplist_convert");
  ASSERT_OK(Env::Default()->CreateDirIfMissing(dir));
  Options options;
  options.cf_paths = {{dir, 0}};
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(NewOffsetMemTable(
      &options, &wb, R"({"mem_cap":16777216,"convert_to_sst":"kDumpMem"})"));
  ASSERT_TRUE(mem->SupportConvertToSST());
  ASSERT_OK(mem->Add(1, kTypeValue, "key1", "v1", nullptr));
  ASSERT_OK(mem->Add(2, kTypeValue, "key2", "v2", nullptr));
  ASSERT_OK(mem->Add(3, kTypeValue, "key3", "v3", nullptr));

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator icmp(BytewiseComparator());
  IntTblPropCollectorFactories collectors;
  TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                          options.compression, options.compression_opts, 0,
                          "default", 0);
  FileMetaData meta;
  meta.fd = FileDescriptor(1, 0, 0);
  meta.num_entries = 3;
  meta.raw_key_size = 12;
  meta.raw_value_size = 6;
  mem->MarkImmutable();
  ASSERT_OK(mem->ConvertToSST(&meta, tbo));
  ASSERT_GT(meta.fd.GetFileSize(), 0U);

  std::string fname = TableFileName(options.cf_paths, 1, 0);
  std::unique_ptr<FSRandomAccessFile> file;
  FileOptions fopt;
  ASSERT_OK(options.env->GetFileSystem()->NewRandomAccessFile(fname, fopt,
                                                              &file, nullptr));
  std::unique_ptr<RandomAccessFileReader> reader(
      new RandomAccessFileReader(std::move(file), fname));
  uint64_t fsize = 0;
  ASSERT_OK(options.env->GetFileSize(fname, &fsize));
  std::unique_ptr<TableFactory> tf(
      EasyNewTableFactory("OffsetSkipListTable", "{}"));
  EnvOptions env_opt;
  TableReaderOptions tro(ioptions, options.prefix_extractor, env_opt, icmp, 0);
  std::unique_ptr<TableReader> table;
  ASSERT_OK(tf->NewTableReader(ReadOptions(), tro, std::move(reader), fsize,
                               &table, true));
  ASSERT_NE(table, nullptr);

  std::vector<TableReader::Anchor> anchors;
  ASSERT_OK(table->ApproximateKeyAnchors(ReadOptions(), anchors));
  ASSERT_FALSE(anchors.empty());
  InternalKey ik1("key1", 1, kTypeValue);
  InternalKey ik3("key3", 3, kTypeValue);
  uint64_t off1 = table->ApproximateOffsetOf(ReadOptions(), ik1.Encode(),
                                             TableReaderCaller::kUncategorized);
  uint64_t off3 = table->ApproximateOffsetOf(ReadOptions(), ik3.Encode(),
                                             TableReaderCaller::kUncategorized);
  ASSERT_LE(off1, off3);
  ASSERT_GT(table->ApproximateSize(ReadOptions(), ik1.Encode(), ik3.Encode(),
                                   TableReaderCaller::kUncategorized),
            0U);
  ASSERT_EQ(table->ApproximateMemoryUsage(), fsize);
  {
    auto tp = table->GetTableProperties();
    ASSERT_EQ(tp->tag_size, 8U * 3U);
    ASSERT_EQ(tp->gdic_size, 0U);
    ASSERT_GE(tp->data_size, 6U);
    ASSERT_GT(tp->index_size, 0U);
  }

  Arena arena;
  InternalIterator* it =
      table->NewIterator(ReadOptions(), nullptr, &arena, true,
                         TableReaderCaller::kUncategorized, 0, false);
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key1");
  ASSERT_EQ(it->value().ToString(), "v1");
  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key2");
  ASSERT_EQ(it->value().ToString(), "v2");
  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key3");
  ASSERT_EQ(it->value().ToString(), "v3");
  it->Next();
  ASSERT_FALSE(it->Valid());
  it->~InternalIterator();
}

TEST(OffsetSkipRepTest, ConvertToSST_ReverseBytewise) {
  std::string dir = test::PerThreadDBPath("offset_skiplist_convert_rev");
  ASSERT_OK(Env::Default()->CreateDirIfMissing(dir));
  Options options;
  options.comparator = ReverseBytewiseComparator();
  options.cf_paths = {{dir, 0}};
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(NewOffsetMemTable(
      &options, &wb, R"({"mem_cap":16777216,"convert_to_sst":"kDumpMem"})"));
  ASSERT_OK(mem->Add(1, kTypeValue, "a", "va", nullptr));
  ASSERT_OK(mem->Add(2, kTypeValue, "b", "vb", nullptr));
  ASSERT_OK(mem->Add(3, kTypeValue, "c", "vc", nullptr));

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator icmp(ReverseBytewiseComparator());
  IntTblPropCollectorFactories collectors;
  TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                          options.compression, options.compression_opts, 0,
                          "default", 0);
  FileMetaData meta;
  meta.fd = FileDescriptor(1, 0, 0);
  meta.num_entries = 3;
  meta.raw_key_size = 3;
  meta.raw_value_size = 6;
  mem->MarkImmutable();
  ASSERT_OK(mem->ConvertToSST(&meta, tbo));
  ASSERT_EQ(meta.smallest.user_key().ToString(), "c");
  ASSERT_EQ(meta.largest.user_key().ToString(), "a");

  std::string fname = TableFileName(options.cf_paths, 1, 0);
  std::unique_ptr<FSRandomAccessFile> file;
  FileOptions fopt;
  ASSERT_OK(options.env->GetFileSystem()->NewRandomAccessFile(fname, fopt,
                                                              &file, nullptr));
  std::unique_ptr<RandomAccessFileReader> reader(
      new RandomAccessFileReader(std::move(file), fname));
  uint64_t fsize = 0;
  ASSERT_OK(options.env->GetFileSize(fname, &fsize));
  std::unique_ptr<TableFactory> tf(
      EasyNewTableFactory("OffsetSkipListTable", "{}"));
  EnvOptions env_opt;
  TableReaderOptions tro(ioptions, options.prefix_extractor, env_opt, icmp, 0);
  std::unique_ptr<TableReader> table;
  ASSERT_OK(tf->NewTableReader(ReadOptions(), tro, std::move(reader), fsize,
                               &table, true));
  ASSERT_NE(table, nullptr);

  Arena arena;
  InternalIterator* it =
      table->NewIterator(ReadOptions(), nullptr, &arena, true,
                         TableReaderCaller::kUncategorized, 0, false);
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "c");
  ASSERT_EQ(it->value().ToString(), "vc");
  it->Next();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "b");
  it->Next();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "a");
  it->Next();
  ASSERT_FALSE(it->Valid());
  it->Seek(InternalKey("bb", 100, kTypeValue).Encode());
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "b");
  it->SeekForPrev(InternalKey("bb", 100, kTypeValue).Encode());
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "c");
  it->~InternalIterator();
}

TEST(OffsetSkipRepTest, ConvertToSST_Empty) {
  std::string dir = test::PerThreadDBPath("offset_skiplist_convert_empty");
  ASSERT_OK(Env::Default()->CreateDirIfMissing(dir));
  Options options;
  options.cf_paths = {{dir, 0}};
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(NewOffsetMemTable(
      &options, &wb, R"({"mem_cap":16777216,"convert_to_sst":"kDumpMem"})"));
  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator icmp(BytewiseComparator());
  IntTblPropCollectorFactories collectors;
  TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                          options.compression, options.compression_opts, 0,
                          "default", 0);
  FileMetaData meta;
  meta.fd = FileDescriptor(1, 0, 0);
  mem->MarkImmutable();
  ASSERT_TRUE(mem->ConvertToSST(&meta, tbo).IsInvalidArgument());
}

TEST(OffsetSkipRepTest, ConvertToSST_FileMmap) {
  Options options;
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(NewOffsetMemTable(
      &options, &wb, R"({"mem_cap":16777216,"convert_to_sst":"kFileMmap"})"));
  ASSERT_TRUE(mem->SupportConvertToSST());
  ASSERT_OK(mem->Add(1, kTypeValue, "key1", "v1", nullptr));
  ASSERT_OK(mem->Add(2, kTypeValue, "key2", "v2", nullptr));
  ASSERT_OK(mem->Add(3, kTypeValue, "key3", "v3", nullptr));

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator icmp(BytewiseComparator());
  IntTblPropCollectorFactories collectors;
  TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                          options.compression, options.compression_opts, 0,
                          "default", 0);
  FileMetaData meta;
  meta.fd = FileDescriptor(1, 0, 0);
  meta.num_entries = 3;
  meta.raw_key_size = 12;
  meta.raw_value_size = 6;
  mem->MarkImmutable();
  ASSERT_OK(mem->ConvertToSST(&meta, tbo));
  ASSERT_GT(meta.fd.GetFileSize(), 0U);

  std::string fname = TableFileName(options.cf_paths, 1, 0);
  std::unique_ptr<FSRandomAccessFile> file;
  FileOptions fopt;
  ASSERT_OK(options.env->GetFileSystem()->NewRandomAccessFile(fname, fopt,
                                                              &file, nullptr));
  std::unique_ptr<RandomAccessFileReader> reader(
      new RandomAccessFileReader(std::move(file), fname));
  uint64_t fsize = 0;
  ASSERT_OK(options.env->GetFileSize(fname, &fsize));
  ASSERT_EQ(fsize, meta.fd.GetFileSize());
  ASSERT_LT(fsize, uint64_t(16) << 20);
  std::unique_ptr<TableFactory> tf(
      EasyNewTableFactory("OffsetSkipListTable", "{}"));
  EnvOptions env_opt;
  TableReaderOptions tro(ioptions, options.prefix_extractor, env_opt, icmp, 0);
  std::unique_ptr<TableReader> table;
  ASSERT_OK(tf->NewTableReader(ReadOptions(), tro, std::move(reader), fsize,
                               &table, true));
  ASSERT_NE(table, nullptr);

  Arena arena;
  InternalIterator* it =
      table->NewIterator(ReadOptions(), nullptr, &arena, true,
                         TableReaderCaller::kUncategorized, 0, false);
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key1");
  ASSERT_EQ(it->value().ToString(), "v1");
  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key2");
  ASSERT_EQ(it->value().ToString(), "v2");
  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key3");
  ASSERT_EQ(it->value().ToString(), "v3");
  it->Next();
  ASSERT_FALSE(it->Valid());
  it->~InternalIterator();
}

TEST(OffsetSkipRepTest, LogRef_ConvertToSST_Reopen) {
  std::string dir = test::PerThreadDBPath("offset_skiplist_logref");
  ASSERT_OK(Env::Default()->CreateDirIfMissing(dir));
  Options options;
  options.cf_paths = {{dir, 0}};
  options.db_paths = {{dir, 0}};
  options.wal_dir = dir;
  options.memtable_as_log_index = true;
  WriteBufferManager wb(options.db_write_buffer_size);

  const std::string v1 = "value-from-wal-1";
  const std::string v2 = "value-from-wal-2";
  std::string payload;
  auto append_wal_kv = [&](const std::string& k, const std::string& v) {
    PutVarint32(&payload, uint32_t(k.size()));
    payload.append(k);
    PutVarint32(&payload, uint32_t(v.size()));
    const size_t val_pos = payload.size();
    payload.append(v);
    return val_pos;
  };
  const size_t pos1 = append_wal_kv("key1", v1);
  const size_t pos2 = append_wal_kv("key2", v2);
  const std::string wal_path = LogFileName(dir, 1);
  ASSERT_OK(
      WriteStringToFile(options.env->GetFileSystem().get(), payload, wal_path));
  auto wal_opened =
      ReadonlyFileMmap::New(*options.env->GetFileSystem(), 1, wal_path);
  ASSERT_TRUE(wal_opened.second.ok()) << wal_opened.second.ToString();
  boost::intrusive_ptr<ReadonlyFileMmap> wal = std::move(wal_opened.first);
  ASSERT_NE(wal, nullptr);
  wal->tail_pos = std::make_shared<uint64_t>(payload.size());

  std::unique_ptr<MemTable> mem(NewOffsetMemTable(
      &options, &wb, R"({"mem_cap":16777216,"convert_to_sst":"kDumpMem"})"));
  ASSERT_TRUE(mem->SupportConvertToSST());

  auto add_logref = [&](SequenceNumber seq, const Slice& ukey, size_t val_pos,
                        const Slice& val) {
    KeyValuePassMemTable kv;
    kv.value = val;
    kv.val_pos = val_pos;
    kv.key_len = uint32_t(ukey.size());
    kv.fileno = 1;
    kv.wal_file = wal.get();
    ASSERT_OK(mem->Add(seq, kTypeValue, ukey,
                       Slice(reinterpret_cast<const char*>(&kv), sizeof(kv)),
                       nullptr));
  };
  add_logref(1, "key1", pos1, Slice(wal->data() + pos1, v1.size()));
  add_logref(2, "key2", pos2, Slice(wal->data() + pos2, v2.size()));

  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;
  Status gs;
  PinnableSlice value;
  ASSERT_TRUE(mem->Get(LookupKey("key1", 100), &value, nullptr, nullptr, &gs,
                       &merge_context, &max_covering_tombstone_seq,
                       ReadOptions(), false));
  ASSERT_OK(gs);
  ASSERT_EQ(value.ToString(), v1);
  value.Reset();
  ASSERT_TRUE(mem->Get(LookupKey("key2", 100), &value, nullptr, nullptr, &gs,
                       &merge_context, &max_covering_tombstone_seq,
                       ReadOptions(), false));
  ASSERT_OK(gs);
  ASSERT_EQ(value.ToString(), v2);

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator icmp(BytewiseComparator());
  IntTblPropCollectorFactories collectors;
  TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                          options.compression, options.compression_opts, 0,
                          "default", 0);
  uint64_t next_blob = 7;
  std::vector<BlobFileAddition> blobs;
  tbo.generate_file_no = [&]() { return next_blob++; };
  tbo.add_blob_file = [&](BlobFileAddition b) {
    blobs.push_back(std::move(b));
  };
  FileMetaData meta;
  meta.fd = FileDescriptor(1, 0, 0);
  meta.num_entries = 2;
  meta.raw_key_size = 8;
  meta.raw_value_size = v1.size() + v2.size();
  mem->MarkImmutable();
  ASSERT_OK(mem->ConvertToSST(&meta, tbo));
  ASSERT_EQ(blobs.size(), 1U);
  ASSERT_EQ(blobs[0].GetBlobFileNumber(), 7U);
  ASSERT_EQ(blobs[0].GetTotalBlobCount(), 2U);
  ASSERT_OK(Env::Default()->FileExists(BlobFileName(dir, 7)));

  mem.reset();
  wal.reset();

  std::string fname = TableFileName(options.cf_paths, 1, 0);
  std::unique_ptr<FSRandomAccessFile> file;
  FileOptions fopt;
  ASSERT_OK(options.env->GetFileSystem()->NewRandomAccessFile(fname, fopt,
                                                              &file, nullptr));
  std::unique_ptr<RandomAccessFileReader> reader(
      new RandomAccessFileReader(std::move(file), fname));
  uint64_t fsize = 0;
  ASSERT_OK(options.env->GetFileSize(fname, &fsize));
  std::unique_ptr<TableFactory> tf(
      EasyNewTableFactory("OffsetSkipListTable", "{}"));
  EnvOptions env_opt;
  TableReaderOptions tro(ioptions, options.prefix_extractor, env_opt, icmp, 0);
  std::unique_ptr<TableReader> table;
  ASSERT_OK(tf->NewTableReader(ReadOptions(), tro, std::move(reader), fsize,
                               &table, true));
  ASSERT_NE(table, nullptr);
  {
    auto tp = table->GetTableProperties();
    ASSERT_EQ(tp->tag_size, 8U * 2U);
    ASSERT_EQ(tp->gdic_size, blobs[0].GetTotalBlobBytes());
    ASSERT_GT(tp->gdic_size, 0U);
    ASSERT_GT(tp->data_size, 0U);
    ASSERT_GT(tp->index_size, 0U);
  }

  Arena arena;
  InternalIterator* it =
      table->NewIterator(ReadOptions(), nullptr, &arena, true,
                         TableReaderCaller::kUncategorized, 0, false);
  it->SeekToFirst();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key1");
  ASSERT_EQ(it->value().ToString(), v1);
  it->Next();
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key2");
  ASSERT_EQ(it->value().ToString(), v2);
  it->Next();
  ASSERT_FALSE(it->Valid());
  it->~InternalIterator();
}

// Worker writes memtable A, A is destroyed, same worker writes B.
// Process-wide thread_local holding Rep* UAF'd here; instance ThreadLocalPtr
// must not.
TEST(OffsetSkipRepTest, LogRef_InstanceTls_ReuseThread) {
  std::string dir = test::PerThreadDBPath("offset_skiplist_logref_tls");
  ASSERT_OK(Env::Default()->CreateDirIfMissing(dir));
  Options options;
  options.cf_paths = {{dir, 0}};
  options.db_paths = {{dir, 0}};
  options.wal_dir = dir;
  options.memtable_as_log_index = true;
  options.allow_concurrent_memtable_write = true;
  WriteBufferManager wb(options.db_write_buffer_size);

  const std::string v1 = "value-from-wal-1";
  const std::string v2 = "value-from-wal-2";
  std::string payload;
  auto append_wal_kv = [&](const std::string& k, const std::string& v) {
    PutVarint32(&payload, uint32_t(k.size()));
    payload.append(k);
    PutVarint32(&payload, uint32_t(v.size()));
    const size_t val_pos = payload.size();
    payload.append(v);
    return val_pos;
  };
  const size_t pos1 = append_wal_kv("key1", v1);
  const size_t pos2 = append_wal_kv("key2", v2);
  const std::string wal_path = LogFileName(dir, 1);
  ASSERT_OK(
      WriteStringToFile(options.env->GetFileSystem().get(), payload, wal_path));
  auto wal_opened =
      ReadonlyFileMmap::New(*options.env->GetFileSystem(), 1, wal_path);
  ASSERT_TRUE(wal_opened.second.ok()) << wal_opened.second.ToString();
  boost::intrusive_ptr<ReadonlyFileMmap> wal = std::move(wal_opened.first);
  ASSERT_NE(wal, nullptr);
  wal->tail_pos = std::make_shared<uint64_t>(payload.size());

  const char* js = R"({"mem_cap":16777216,"convert_to_sst":"kDumpMem"})";
  auto add_to = [&](MemTable* m, SequenceNumber seq, const Slice& ukey,
                    size_t val_pos, const Slice& val) {
    KeyValuePassMemTable kv;
    kv.value = val;
    kv.val_pos = val_pos;
    kv.key_len = uint32_t(ukey.size());
    kv.fileno = 1;
    kv.wal_file = wal.get();
    MemTablePostProcessInfo post;
    return m->Add(seq, kTypeValue, ukey,
                  Slice(reinterpret_cast<const char*>(&kv), sizeof(kv)),
                  nullptr, true, &post);
  };

  std::mutex mu;
  std::condition_variable cv;
  int phase = 0;
  Status worker_st;
  std::unique_ptr<MemTable> mem(NewOffsetMemTable(&options, &wb, js));

  std::thread t([&]() {
    {
      std::unique_lock<std::mutex> lk(mu);
      cv.wait(lk, [&] { return phase >= 1; });
    }
    worker_st = add_to(mem.get(), 1, "key1", pos1,
                       Slice(wal->data() + pos1, v1.size()));
    {
      std::unique_lock<std::mutex> lk(mu);
      phase = 2;
      cv.notify_all();
      cv.wait(lk, [&] { return phase >= 3; });
    }
    worker_st = add_to(mem.get(), 2, "key2", pos2,
                       Slice(wal->data() + pos2, v2.size()));
    {
      std::lock_guard<std::mutex> lk(mu);
      phase = 4;
      cv.notify_all();
    }
  });

  {
    std::unique_lock<std::mutex> lk(mu);
    phase = 1;
    cv.notify_all();
    cv.wait(lk, [&] { return phase >= 2; });
  }
  ASSERT_OK(worker_st);
  mem.reset();
  mem.reset(NewOffsetMemTable(&options, &wb, js));
  {
    std::unique_lock<std::mutex> lk(mu);
    phase = 3;
    cv.notify_all();
    cv.wait(lk, [&] { return phase >= 4; });
  }
  ASSERT_OK(worker_st);
  t.join();

  MergeContext merge_context;
  SequenceNumber max_covering_tombstone_seq = 0;
  Status gs;
  PinnableSlice value;
  ASSERT_TRUE(mem->Get(LookupKey("key2", 100), &value, nullptr, nullptr, &gs,
                       &merge_context, &max_covering_tombstone_seq,
                       ReadOptions(), false));
  ASSERT_OK(gs);
  ASSERT_EQ(value.ToString(), v2);
}

TEST(OffsetSkipRepTest, ConvertToSST_MultiVersion) {
  std::string dir = test::PerThreadDBPath("offset_skiplist_convert_mv");
  ASSERT_OK(Env::Default()->CreateDirIfMissing(dir));
  Options options;
  options.cf_paths = {{dir, 0}};
  WriteBufferManager wb(options.db_write_buffer_size);
  std::unique_ptr<MemTable> mem(NewOffsetMemTable(
      &options, &wb, R"({"mem_cap":16777216,"convert_to_sst":"kDumpMem"})"));
  ASSERT_OK(mem->Add(1, kTypeValue, "k", "v1", nullptr));
  ASSERT_OK(mem->Add(3, kTypeValue, "k", "v3", nullptr));
  ASSERT_OK(mem->Add(2, kTypeValue, "k", "v2", nullptr));
  ASSERT_OK(mem->Add(4, kTypeValue, "z", "vz", nullptr));

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator icmp(BytewiseComparator());
  IntTblPropCollectorFactories collectors;
  TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                          options.compression, options.compression_opts, 0,
                          "default", 0);
  FileMetaData meta;
  meta.fd = FileDescriptor(1, 0, 0);
  meta.num_entries = 4;
  mem->MarkImmutable();
  ASSERT_OK(mem->ConvertToSST(&meta, tbo));
  ASSERT_EQ(ExtractUserKey(meta.smallest.Encode()).ToString(), "k");
  ASSERT_EQ(ExtractUserKey(meta.largest.Encode()).ToString(), "z");

  std::string fname = TableFileName(options.cf_paths, 1, 0);
  std::unique_ptr<FSRandomAccessFile> file;
  FileOptions fopt;
  ASSERT_OK(options.env->GetFileSystem()->NewRandomAccessFile(fname, fopt,
                                                              &file, nullptr));
  std::unique_ptr<RandomAccessFileReader> reader(
      new RandomAccessFileReader(std::move(file), fname));
  uint64_t fsize = 0;
  ASSERT_OK(options.env->GetFileSize(fname, &fsize));
  std::unique_ptr<TableFactory> tf(
      EasyNewTableFactory("OffsetSkipListTable", "{}"));
  EnvOptions env_opt;
  TableReaderOptions tro(ioptions, options.prefix_extractor, env_opt, icmp, 0);
  std::unique_ptr<TableReader> table;
  ASSERT_OK(tf->NewTableReader(ReadOptions(), tro, std::move(reader), fsize,
                               &table, true));

  Arena arena;
  InternalIterator* it =
      table->NewIterator(ReadOptions(), nullptr, &arena, true,
                         TableReaderCaller::kUncategorized, 0, false);
  it->SeekToFirst();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "k");
  ASSERT_EQ(it->value().ToString(), "v3");
  it->Next();
  ASSERT_EQ(it->value().ToString(), "v2");
  it->Next();
  ASSERT_EQ(it->value().ToString(), "v1");
  it->Next();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "z");
  ASSERT_EQ(it->value().ToString(), "vz");
  it->SeekForPrev(InternalKey("m", 100, kTypeValue).Encode());
  ASSERT_TRUE(it->Valid());
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "k");
  it->~InternalIterator();
}

TEST(OffsetSkipRepTest, LogRef_InlineNoBlob) {
  std::string dir = test::PerThreadDBPath("offset_skiplist_logref_inline");
  ASSERT_OK(Env::Default()->CreateDirIfMissing(dir));
  Options options;
  options.cf_paths = {{dir, 0}};
  options.db_paths = {{dir, 0}};
  options.wal_dir = dir;
  options.memtable_as_log_index = true;
  WriteBufferManager wb(options.db_write_buffer_size);

  const std::string v1 = "abc";
  const std::string v2 = "defghij";
  std::string payload;
  const size_t pos1 = AppendWalKv(&payload, "key1", v1);
  const size_t pos2 = AppendWalKv(&payload, "key2", v2);
  const std::string wal_path = LogFileName(dir, 1);
  ASSERT_OK(
      WriteStringToFile(options.env->GetFileSystem().get(), payload, wal_path));
  auto wal_opened =
      ReadonlyFileMmap::New(*options.env->GetFileSystem(), 1, wal_path);
  ASSERT_TRUE(wal_opened.second.ok()) << wal_opened.second.ToString();
  boost::intrusive_ptr<ReadonlyFileMmap> wal = std::move(wal_opened.first);
  wal->tail_pos = std::make_shared<uint64_t>(payload.size());

  std::unique_ptr<MemTable> mem(NewOffsetMemTable(
      &options, &wb, R"({"mem_cap":16777216,"convert_to_sst":"kDumpMem"})"));
  AddLogRef(mem.get(), 1, "key1", pos1, Slice(wal->data() + pos1, v1.size()),
            wal.get());
  AddLogRef(mem.get(), 2, "key2", pos2, Slice(wal->data() + pos2, v2.size()),
            wal.get());
  std::string value;
  ASSERT_TRUE(MemGet(mem.get(), "key1", 100, &value));
  ASSERT_EQ(value, v1);
  ASSERT_TRUE(MemGet(mem.get(), "key2", 100, &value));
  ASSERT_EQ(value, v2);

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator icmp(BytewiseComparator());
  IntTblPropCollectorFactories collectors;
  TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                          options.compression, options.compression_opts, 0,
                          "default", 0);
  std::vector<BlobFileAddition> blobs;
  tbo.generate_file_no = []() { return uint64_t{1}; };
  tbo.add_blob_file = [&](BlobFileAddition b) {
    blobs.push_back(std::move(b));
  };
  FileMetaData meta;
  meta.fd = FileDescriptor(1, 0, 0);
  meta.num_entries = 2;
  mem->MarkImmutable();
  ASSERT_OK(mem->ConvertToSST(&meta, tbo));
  ASSERT_TRUE(blobs.empty());

  mem.reset();
  wal.reset();
  std::string fname = TableFileName(options.cf_paths, 1, 0);
  std::unique_ptr<FSRandomAccessFile> file;
  FileOptions fopt;
  ASSERT_OK(options.env->GetFileSystem()->NewRandomAccessFile(fname, fopt,
                                                              &file, nullptr));
  std::unique_ptr<RandomAccessFileReader> reader(
      new RandomAccessFileReader(std::move(file), fname));
  uint64_t fsize = 0;
  ASSERT_OK(options.env->GetFileSize(fname, &fsize));
  std::unique_ptr<TableFactory> tf(
      EasyNewTableFactory("OffsetSkipListTable", "{}"));
  EnvOptions env_opt;
  TableReaderOptions tro(ioptions, options.prefix_extractor, env_opt, icmp, 0);
  std::unique_ptr<TableReader> table;
  ASSERT_OK(tf->NewTableReader(ReadOptions(), tro, std::move(reader), fsize,
                               &table, true));
  Arena arena;
  InternalIterator* it =
      table->NewIterator(ReadOptions(), nullptr, &arena, true,
                         TableReaderCaller::kUncategorized, 0, false);
  it->SeekToFirst();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key1");
  ASSERT_EQ(it->value().ToString(), v1);
  it->Next();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key2");
  ASSERT_EQ(it->value().ToString(), v2);
  it->Next();
  ASSERT_FALSE(it->Valid());
  it->~InternalIterator();
}

TEST(OffsetSkipRepTest, LogRef_PlainAndSameKey) {
  std::string dir = test::PerThreadDBPath("offset_skiplist_logref_plain");
  ASSERT_OK(Env::Default()->CreateDirIfMissing(dir));
  Options options;
  options.cf_paths = {{dir, 0}};
  options.db_paths = {{dir, 0}};
  options.wal_dir = dir;
  options.memtable_as_log_index = true;
  WriteBufferManager wb(options.db_write_buffer_size);

  const std::string v1 = "value-from-wal-1";
  const std::string v2 = "value-from-wal-2";
  const std::string v3 = "value-from-wal-3";
  std::string payload;
  const size_t pos1 = AppendWalKv(&payload, "key", v1);
  const size_t pos2 = AppendWalKv(&payload, "key", v2);
  const size_t pos3 = AppendWalKv(&payload, "key", v3);
  const std::string wal_path = LogFileName(dir, 1);
  ASSERT_OK(
      WriteStringToFile(options.env->GetFileSystem().get(), payload, wal_path));
  auto wal_opened =
      ReadonlyFileMmap::New(*options.env->GetFileSystem(), 1, wal_path);
  ASSERT_TRUE(wal_opened.second.ok()) << wal_opened.second.ToString();
  boost::intrusive_ptr<ReadonlyFileMmap> wal = std::move(wal_opened.first);
  wal->tail_pos = std::make_shared<uint64_t>(payload.size());

  std::unique_ptr<MemTable> mem(NewOffsetMemTable(
      &options, &wb,
      R"({"mem_cap":16777216,"convert_to_sst":"kDumpMem","log_ref_format":"kPlainLogRef"})"));
  AddLogRef(mem.get(), 1, "key", pos1, Slice(wal->data() + pos1, v1.size()),
            wal.get());
  AddLogRef(mem.get(), 3, "key", pos3, Slice(wal->data() + pos3, v3.size()),
            wal.get());
  AddLogRef(mem.get(), 2, "key", pos2, Slice(wal->data() + pos2, v2.size()),
            wal.get());
  std::string value;
  ASSERT_TRUE(MemGet(mem.get(), "key", 100, &value));
  ASSERT_EQ(value, v3);
  ASSERT_TRUE(MemGet(mem.get(), "key", 2, &value));
  ASSERT_EQ(value, v2);

  ImmutableOptions ioptions(options);
  MutableCFOptions moptions(options);
  InternalKeyComparator icmp(BytewiseComparator());
  IntTblPropCollectorFactories collectors;
  TableBuilderOptions tbo(ioptions, moptions, icmp, &collectors,
                          options.compression, options.compression_opts, 0,
                          "default", 0);
  uint64_t next_blob = 9;
  std::vector<BlobFileAddition> blobs;
  tbo.generate_file_no = [&]() { return next_blob++; };
  tbo.add_blob_file = [&](BlobFileAddition b) {
    blobs.push_back(std::move(b));
  };
  FileMetaData meta;
  meta.fd = FileDescriptor(1, 0, 0);
  meta.num_entries = 3;
  mem->MarkImmutable();
  ASSERT_OK(mem->ConvertToSST(&meta, tbo));
  ASSERT_EQ(blobs.size(), 1U);
  ASSERT_EQ(blobs[0].GetTotalBlobCount(), 3U);

  mem.reset();
  wal.reset();
  std::string fname = TableFileName(options.cf_paths, 1, 0);
  std::unique_ptr<FSRandomAccessFile> file;
  FileOptions fopt;
  ASSERT_OK(options.env->GetFileSystem()->NewRandomAccessFile(fname, fopt,
                                                              &file, nullptr));
  std::unique_ptr<RandomAccessFileReader> reader(
      new RandomAccessFileReader(std::move(file), fname));
  uint64_t fsize = 0;
  ASSERT_OK(options.env->GetFileSize(fname, &fsize));
  std::unique_ptr<TableFactory> tf(
      EasyNewTableFactory("OffsetSkipListTable", "{}"));
  EnvOptions env_opt;
  TableReaderOptions tro(ioptions, options.prefix_extractor, env_opt, icmp, 0);
  std::unique_ptr<TableReader> table;
  ASSERT_OK(tf->NewTableReader(ReadOptions(), tro, std::move(reader), fsize,
                               &table, true));
  Arena arena;
  InternalIterator* it =
      table->NewIterator(ReadOptions(), nullptr, &arena, true,
                         TableReaderCaller::kUncategorized, 0, false);
  it->SeekToFirst();
  ASSERT_EQ(ExtractUserKey(it->key()).ToString(), "key");
  ASSERT_EQ(it->value().ToString(), v3);
  it->Next();
  ASSERT_EQ(it->value().ToString(), v2);
  it->Next();
  ASSERT_EQ(it->value().ToString(), v1);
  it->Next();
  ASSERT_FALSE(it->Valid());
  it->~InternalIterator();
}

}  // namespace ROCKSDB_NAMESPACE

int main(int argc, char** argv) {
  ROCKSDB_NAMESPACE::port::InstallStackTraceHandler();
  ::testing::InitGoogleTest(&argc, argv);
  return RUN_ALL_TESTS();
}
