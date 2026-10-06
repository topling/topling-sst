#pragma once

#include "db/version_edit.h"
#include "test_util/sync_point.h"
#include <terark/util/atomic.hpp>

namespace ROCKSDB_NAMESPACE {

// One permanent mempool block per FileMmap writer TLS. Offsets use 4-byte units.
// Counts include physical entries beyond pubseq, so they can exceed the
// expected totals for the pubseq-filtered view.
// A crash can interrupt accounting of an in-flight insertion.
struct alignas(8) MemTableRepStats {
  struct WalSlot {
    uint64_t fileno;
    uint64_t cnt;
    uint64_t bytes;
  };

  uint64_t num_entries = 0;
  uint64_t num_deletions = 0;
  uint64_t num_merges = 0;
  uint64_t raw_key_size = 0;
  uint64_t raw_value_size = 0;
  static constexpr size_t kMaxWals = 16;
  // Cumulative WAL counts; never cleared.
  struct LogFileCntBytesThreadLocal {
    uint64_t cnt = 0;
    uint64_t bytes = 0;
  } wals[kMaxWals];
  uint32_t next = 0;

  static MemTableRepStats* Link(uint8_t* base, uint32_t pos, uint32_t& head) {
    ROCKSDB_ASSERT_NE(pos, 0U);
    ROCKSDB_ASSERT_EQ(pos % 2, 0U);
    auto* node = new (base + size_t(pos) * 4) MemTableRepStats;
    node->next = terark::as_atomic(head).load(std::memory_order_relaxed);
    TEST_SYNC_POINT("MemTableStats::BeforePublish");
    while (!terark::as_atomic(head).compare_exchange_weak(
        node->next, pos, std::memory_order_release, std::memory_order_relaxed)) {}
    TEST_SYNC_POINT("MemTableStats::AfterPublish");
    return node;
  }

  void Add(uint64_t tag, size_t key_size, size_t value_size) {
    num_entries++;
    const auto type = static_cast<ValueType>(tag & 0xff);
    num_deletions += type == kTypeDeletion || type == kTypeSingleDeletion ||
                     type == kTypeDeletionWithTimestamp;
    num_merges += type == kTypeMerge;
    raw_key_size += key_size + 8;
    raw_value_size += value_size;
  }

  // Recovered counts may exceed the actual counts of pubseq-visible data:
  // cumulative counters also include entries beyond pubseq.
  static void Recover(const uint8_t* base, uint32_t head, FileMetaData* meta,
                      WalSlot* wals, size_t num_wals) {
    if (meta) {
      meta->num_entries = 0;
      meta->num_deletions = 0;
      meta->num_merges = 0;
      meta->raw_key_size = 0;
      meta->raw_value_size = 0;
    }
    for (size_t i = 0; i < num_wals; i++) {
      wals[i].cnt = 0;
      wals[i].bytes = 0;
    }
    TEST_SYNC_POINT("MemTableStats::RecoverWals:AfterReset");
    while (head) {
      auto* node = reinterpret_cast<const MemTableRepStats*>(base + size_t(head) * 4);
      if (meta) {
        meta->num_entries += node->num_entries;
        meta->num_deletions += node->num_deletions;
        meta->num_merges += node->num_merges;
        meta->raw_key_size += node->raw_key_size;
        meta->raw_value_size += node->raw_value_size;
      }
      for (size_t i = 0; i < num_wals; i++) {
        wals[i].cnt += node->wals[i].cnt;
        TEST_SYNC_POINT("MemTableStats::RecoverWals:AfterCount");
        wals[i].bytes += node->wals[i].bytes;
        TEST_SYNC_POINT("MemTableStats::RecoverWals:AfterBytes");
      }
      TEST_SYNC_POINT("MemTableStats::RecoverWals:AfterNode");
      head = node->next;
    }
  }
};

}  // namespace ROCKSDB_NAMESPACE
