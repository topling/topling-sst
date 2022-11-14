#include <rocksdb/table.h>
#include <db/table_properties_collector.h>
#include <table/table_builder.h>
#include <table/block_based/block_builder.h>

#include "top_table_common.h"

namespace rocksdb {
class TopTableBuilderBase : public TableBuilder {
protected:
  const ImmutableOptions& ioptions_;
  std::vector<std::unique_ptr<IntTblPropCollector> > collectors_;
  WritableFileWriter* file_ = nullptr;
  uint64_t offset_ = 0;
  size_t num_user_key_ = 0;
  Status status_;
  IOStatus io_status_;
  TableProperties properties_;
  BlockBuilder range_del_block_;
  bool closed_ = false;  // Either Finish() or Abandon() has been called.
  bool isReverseBytewiseOrder_;

  TopTableBuilderBase(const TableBuilderOptions& tbo, WritableFileWriter* file);

  Status status() const final { return status_; }
  uint64_t NumEntries() const final { return properties_.num_entries; }
  uint64_t FileSize() const override { assert(closed_); return offset_; }
  virtual uint64_t EstimatedFileSize() const override = 0;
  TableProperties GetTableProperties() const final { return properties_; }
  IOStatus io_status() const final { return io_status_; }
  std::string GetFileChecksum() const final;
  const char* GetFileChecksumFuncName() const final;
  bool NeedCompact() const override;
  void WriteMeta(uint64_t magic, std::initializer_list<std::pair<const std::string&, BlockHandle>> blocks);
  void FinishAsEmptyTable();
};

template<class ByteArray>
static
BlockHandle WriteBlock(const ByteArray& data, WritableFileWriter* file, uint64_t* offset) {
  IOStatus s = file->Append(SliceOf(data));
  if (!s.ok()) {
    throw s; // NOLINT
  }
  uint64_t old_offset = *offset;
  *offset += data.size();
  return BlockHandle(old_offset, data.size());
}
void WriteFileFooter(uint64_t magic, const Slice& meta,  WritableFileWriter*, uint64_t* offset);

} // namespace rocksdb