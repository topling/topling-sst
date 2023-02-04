//
// Created by leipeng on 2022-10-26 16:44
//
#pragma once

#include <topling/json_fwd.h>
#include <rocksdb/file_system.h> // for FSRandomAccessFile
#include <rocksdb/options.h>
#include <rocksdb/table.h>
#include <table/table_reader.h>
#include <table/table_builder.h> // for TableReaderOptions
#include <db/range_tombstone_fragmenter.h>

namespace rocksdb {

using nlohmann::json;

ROCKSDB_ENUM_CLASS(WarmupLevel, unsigned char, kNone, kIndex, kValue);

struct BlockContents ReadMetaBlockE(class RandomAccessFileReader* file,
  uint64_t file_size,
  uint64_t table_magic_number,
  const struct ImmutableOptions& ioptions,
  const std::string& meta_block_name);

class MmapReadWrapper : public FSRandomAccessFile {
public:
  Slice mmap_;
  FSRandomAccessFile* target_;
  MmapReadWrapper(RandomAccessFileReader*, bool populate = false);
  MmapReadWrapper(const std::unique_ptr<RandomAccessFileReader>& r, bool populate = false)
    : MmapReadWrapper(r.get(), populate) {}
  ~MmapReadWrapper();
  ptrdiff_t FileDescriptor() const final;
  IOStatus Read(uint64_t offset, size_t n, const IOOptions& options,
                Slice* result, char* scratch, IODebugContext*) const final;
};

class TopTableReaderBase : public TableReader, boost::noncopyable {
protected:
  std::unique_ptr<RandomAccessFileReader> file_;
  std::shared_ptr<TableProperties> table_properties_;
#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 70090
  std::shared_ptr<FragmentedRangeTombstoneList> fragmented_range_dels_;
#else
  std::shared_ptr<const FragmentedRangeTombstoneList> fragmented_range_dels_;
#endif
  SequenceNumber global_seqno_ = kDisableGlobalSequenceNumber;
  Slice file_data_;
  bool isReverseBytewiseOrder_ = false;
  bool advise_random_on_open_ = false;
  SequenceNumber GetSequenceNumber() const { return global_seqno_; }
  void LoadTombstone(RandomAccessFileReader*, const TableReaderOptions&, uint64_t file_size, uint64_t magic);

public:
  FragmentedRangeTombstoneIterator* NewRangeTombstoneIterator(const ReadOptions&) override;
  size_t ApproximateMemoryUsage() const override { return file_data_.size(); }
  std::shared_ptr<const TableProperties>
  GetTableProperties() const override { return table_properties_; }
  void SetupForCompaction() override {}
  void Prepare(const Slice& target) override {}

#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 70060
  Status ApproximateKeyAnchors(const ReadOptions&, std::vector<Anchor>&) override;
#endif

  ~TopTableReaderBase() override;

  InternalIterator* EasyNewIter();

  void LoadCommonPart(RandomAccessFileReader*, const TableReaderOptions&, Slice file_data, uint64_t magic);
  virtual std::string ToWebViewString(const json& dump_options) const = 0;

  mutable ptrdiff_t cumu_iter_num_ = 0; // intentional public
  mutable ptrdiff_t live_iter_num_ = 0; // intentional public
  mutable size_t  iter_seek_cnt = 0;
  mutable size_t  iter_next_cnt = 0;
  mutable size_t  iter_prev_cnt = 0;
  mutable size_t  iter_key_len = 0;
  mutable size_t  iter_val_len = 0;
};

class TopEmptyTableReader : public TopTableReaderBase {
  class Iter;
public:
  InternalIterator*
  NewIterator(const ReadOptions&, const SliceTransform* prefix_extractor, Arena* a,
              bool skip_filters, TableReaderCaller caller,
              size_t compaction_readahead_size,
              bool allow_unprepared_value) override;
  Status Get(const ReadOptions&, const Slice& key, GetContext*,
             const SliceTransform*, bool skip_filters) override;
  uint64_t ApproximateOffsetOf(const Slice&, TableReaderCaller) override { return 0; }
  uint64_t ApproximateSize(const Slice&, const Slice&, TableReaderCaller) override { return 0; }

  void Open(RandomAccessFileReader*, Slice file_data, const TableReaderOptions&);
  std::string ToWebViewString(const json& dump_options) const override {
    return "TopEmptyTableReader";
  }
};

extern const std::string kTopEmptyTableKey;
extern const uint64_t kTopEmptyTableMagicNumber;

} // namespace rocksdb
