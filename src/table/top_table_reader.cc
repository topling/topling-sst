//
// Created by leipeng on 2022-10-26 16:44
//
#include "top_table_reader.h"
#include "top_table_common.h"
#include <rocksdb/memory_allocator.h>
#include <rocksdb/table_properties.h>
#include <rocksdb/snapshot.h>
#include <table/block_based/block.h>
#include <table/meta_blocks.h>
#include <terark/util/vm_util.hpp>

#ifdef _MSC_VER
# define NOMINMAX
# define WIN32_LEAN_AND_MEAN  // We only need minimal includes
# include <Windows.h>
# undef min
# undef max
#else
# include <fcntl.h>
# include <sys/mman.h>
# include <sys/unistd.h>
#endif

#if defined(__linux__)
#include <linux/mman.h>
#endif

namespace rocksdb {

void MlockBytes(const void* addr, size_t len) {
  auto base = terark::pow2_align_down(uintptr_t(addr), 4096);
  auto size = terark::pow2_align_up(uintptr_t(addr) + len, 4096) - base;
#if defined(_MSC_VER)
  VirtualLock((const void*)base, size);
#else
  mlock((const void*)base, size); // ignore error
#endif
}

ROCKSDB_ENUM_CLASS(WarmupProvider, int, populate, willneed, mlock, touch);

void MmapWarmUpBytes(const void* addr, size_t len) {
  const static auto provider = []{
    const char* env = getenv("TOPLINGDB_WARMUP_PROVIDER");
    if (terark::g_has_madv_populate)
      return enum_value(env ? env : "populate", WarmupProvider::populate);
    else
      return enum_value(env ? env : "willneed", WarmupProvider::willneed);
  }();
  auto base = terark::pow2_align_down(uintptr_t(addr), 4096);
  auto size = terark::pow2_align_up(uintptr_t(addr) + len, 4096) - base;
#ifdef POSIX_MADV_WILLNEED
  switch (provider) {
  case WarmupProvider::willneed:
    posix_madvise((void*)base, size, POSIX_MADV_WILLNEED);
    break;
  case WarmupProvider::populate:
    // MADV_POPULATE_READ is 22, and it is just supported by kernel 5.14+
    #define MY_POPULATE_READ 22
    if (madvise((void*)base, size, MY_POPULATE_READ) != 0) {
      if (EINVAL == errno) // MADV_POPULATE_READ is not supported
        goto mlock_unlock;
    }
    break;
  case WarmupProvider::mlock: mlock_unlock:
    if (mlock((void*)base, size) == 0) {
      munlock((void*)base, size);
    }
    break;
  case WarmupProvider::touch: // MADV_WILLNEED before touch
    posix_madvise((void*)base, size, POSIX_MADV_WILLNEED);
    goto touch_pages;
  }
#elif defined(_MSC_VER)
  WIN32_MEMORY_RANGE_ENTRY vm;
  vm.VirtualAddress = (void*)base;
  vm.NumberOfBytes  = size;
  PrefetchVirtualMemory(GetCurrentProcess(), 1, &vm, 0);
#else
#endif
  return;
touch_pages:
  size_t sum_unused = 0;
  for (size_t i = 0; i < size; i += 4096) {
    byte_t unused = ((const volatile byte_t*)base)[i];
    sum_unused += unused;
  }
  TERARK_UNUSED_VAR(sum_unused);
}

void MmapColdizeBytes(const void* addr, size_t len) {
  size_t low = terark::pow2_align_up(size_t(addr), 4096);
  size_t hig = terark::pow2_align_down(size_t(addr) + len, 4096);
  if (low < hig) {
    size_t size = hig - low;
#ifdef MADV_DONTNEED
    madvise((void*)low, size, MADV_DONTNEED);
#elif defined(_MSC_VER) // defined(_WIN32) || defined(_WIN64)
    VirtualUnlock((void*)low, size);
#endif
  }
}

// defined in block_based_table_reader.cc
Status GetGlobalSequenceNumber(const TableProperties& table_properties,
                               SequenceNumber largest_seqno,
                               SequenceNumber* seqno);

Block* DetachBlockContents(BlockContents &tombstoneBlock, SequenceNumber global_seqno)
{
  auto tombstoneBuf = AllocateBlock(tombstoneBlock.data.size(), nullptr);
  memcpy(tombstoneBuf.get(), tombstoneBlock.data.data(), tombstoneBlock.data.size());
  MmapColdize(tombstoneBlock.data);
  return new Block(
    BlockContents(std::move(tombstoneBuf), tombstoneBlock.data.size()),
    global_seqno);
}

void TopTableReaderBase::
LoadTombstone(RandomAccessFileReader* file, uint64_t file_size, uint64_t magic)
try {
  BlockContents tombstoneBlock = ReadMetaBlockE(file, file_size, magic,
      table_reader_options_.ioptions,  kRangeDelBlock);
  TERARK_VERIFY(!tombstoneBlock.data.empty());
  auto  block = DetachBlockContents(tombstoneBlock, GetSequenceNumber());
  auto& icomp = table_reader_options_.internal_comparator;
  auto* ucomp = icomp.user_comparator();
  auto  diter = UniquePtrOf(block->NewDataIterator(ucomp, global_seqno_));
  auto  delfn = [](void* arg0, void*) { delete static_cast<Block*>(arg0); };
  diter->RegisterCleanup(delfn, block, nullptr);
  fragmented_range_dels_ =
      std::make_shared<FragmentedRangeTombstoneList>(std::move(diter), icomp);
}
catch (const Status&) {
  // do nothing, when not found the block, Status is Corruption,
  // it is confused with real error
}

FragmentedRangeTombstoneIterator*
TopTableReaderBase::NewRangeTombstoneIterator(const ReadOptions& ro) {
  if (fragmented_range_dels_ == nullptr) {
    return nullptr;
  }
  SequenceNumber snapshot = kMaxSequenceNumber;
  if (ro.snapshot != nullptr) {
    snapshot = ro.snapshot->GetSequenceNumber();
  }
  return new FragmentedRangeTombstoneIterator(fragmented_range_dels_,
                        table_reader_options_.internal_comparator, snapshot);
}

#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 60280
inline
Status ReadTableProperties(RandomAccessFileReader* file, uint64_t file_size,
                           uint64_t table_magic_number,
                           const ImmutableOptions& ioptions,
                           TableProperties** properties) {
  std::unique_ptr<TableProperties> prop;
  MemoryAllocator* memory_allocator = nullptr;
  FilePrefetchBuffer* prefetch_buffer = nullptr;
  Status s = ReadTableProperties(file, file_size, table_magic_number,
                  ioptions, &prop, memory_allocator, prefetch_buffer);
  *properties = prop.release();
  return s;
}
#endif

void TopTableReaderBase::LoadCommonPart(RandomAccessFileReader* file,
                                Slice file_data, uint64_t magic) {
  uint64_t file_size = file_data.size_;
  file_data_ = file_data;
  file_.reset(file); // take ownership
  const auto& ioptions = table_reader_options_.ioptions;
  TableProperties* props = nullptr;
  Status s = ReadTableProperties(file, file_size, magic, ioptions, &props);
  if (!s.ok()) {
    throw s; // NOLINT
  }
  TERARK_VERIFY(nullptr != props);
  table_properties_.reset(props);
  //TERARK_VERIFY(table_reader_options_.env_options.use_mmap_reads);

// verify comparator
  TERARK_VERIFY_F(IsBytewiseComparator(ioptions.user_comparator),
                  "Name(): %s", ioptions.user_comparator->Name());
  TERARK_VERIFY_F(IsBytewiseComparator(props->comparator_name),
                  "Name(): %s", props->comparator_name.c_str());
  if (IsForwardBytewiseComparator(props->comparator_name) !=
      IsForwardBytewiseComparator(ioptions.user_comparator))
  {
    throw Status::InvalidArgument(ROCKSDB_FUNC,
      "Invalid user_comparator , need " + props->comparator_name
      + ", but provide " + ioptions.user_comparator->Name());
  }
  TERARK_VERIFY_EQ(table_reader_options_.internal_comparator.user_comparator(),
                   table_reader_options_.ioptions.user_comparator);
// verify comparator end

  isReverseBytewiseOrder_ = !IsForwardBytewiseComparator(ioptions.user_comparator);
  GetGlobalSequenceNumber(*props, table_reader_options_.largest_seqno, &global_seqno_);
  if (global_seqno_ >= kMaxSequenceNumber) {
    global_seqno_ = 0;
  }
  Debug(ioptions.info_log,
       "TopTableReaderBase::LoadCommonPart(%s): global_seqno = %" PRIu64,
       file->file_name().c_str(), global_seqno_);
  LoadTombstone(file, file_size, magic);
}

TopTableReaderBase::~TopTableReaderBase() {
  TERARK_VERIFY_F(0 == live_iter_num_, "real: %zd", live_iter_num_);
}

#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 70060
Status TopTableReaderBase::
ApproximateKeyAnchors(const ReadOptions&, std::vector<Anchor>& anchors) {
  std::vector<std::string> rand_keys;
  Status s;
  if (GetRandomInteranlKeysAppend(128, &rand_keys)) {
    for (auto& key : rand_keys) { // convert internal key to user key
      key.resize(key.size() - 8); // remove SeqNum+Type
    }
    std::sort(rand_keys.begin(), rand_keys.end());
    if (this->isReverseBytewiseOrder_) {
      std::reverse(rand_keys.begin(), rand_keys.end());
    }
    const size_t avg_size = file_data_.size_ / rand_keys.size();
    anchors.reserve(rand_keys.size());
    for (const auto& user_key : rand_keys) {
      anchors.emplace_back(user_key, avg_size);
    }
  }
  else {
    s = Status::NotSupported("GetRandomInteranlKeysAppend() not supported.");
  }
  return s;
}
#endif

///////////////////////////////////////////////////////////////////////////////
void
TopEmptyTableReader::Open(RandomAccessFileReader* file, Slice file_data) {
  LoadCommonPart(file, file_data, kTopEmptyTableMagicNumber);
  auto props = table_properties_.get();
  ROCKS_LOG_DEBUG(table_reader_options_.ioptions.info_log
    , "TopEmptyTableReader::Open(%s): fsize = %zd, entries = %zd keys = 0 indexSize = 0 valueSize = 0, warm up time = 0.000'sec, build cache time =      0.000'sec\n"
    , file->file_name().c_str()
    , size_t(file_data.size_), size_t(props->num_entries)
  );
}

class TopEmptyTableReader::Iter : public InternalIterator, boost::noncopyable {
public:
  void SetPinnedItersMgr(PinnedIteratorsManager*) override {}
  bool Valid() const override { return false; }
  void SeekToFirst() override {}
  void SeekToLast() override {}
  void SeekForPrev(const Slice&) override {}
  void Seek(const Slice&) override {}
  void Next() override {}
  void Prev() override {}
  Slice key() const override { THROW_STD(invalid_argument, "Invalid call"); }
  Slice value() const override { THROW_STD(invalid_argument, "Invalid call"); }
  Status status() const override { return Status::OK(); }
  bool IsKeyPinned() const override { return false; }
  bool IsValuePinned() const override { return false; }
};
InternalIterator*
TopEmptyTableReader::NewIterator(
            const ReadOptions&, const SliceTransform*, Arena* a,
            bool skip_filters, TableReaderCaller caller,
            size_t compaction_readahead_size,
            bool allow_unprepared_value) {
  return a ? new(a->AllocateAligned(sizeof(Iter)))Iter() : new Iter();
}
Status TopEmptyTableReader::Get(const ReadOptions&, const Slice&,
                                   GetContext*, const SliceTransform*, bool) {
  return Status::OK();
}

///////////////////////////////////////////////////////////////////////////////

} // namespace rocksdb
