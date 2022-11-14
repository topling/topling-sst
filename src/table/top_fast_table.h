//
// Created by leipeng on 2020/8/23.
//

#pragma once
#include <rocksdb/table.h>
#include <boost/intrusive_ptr.hpp>
#include <memory>
#include <vector>
#include <topling/side_plugin_repo.h>
#include "top_table_reader.h"

namespace ROCKSDB_NAMESPACE {

ROCKSDB_ENUM_CLASS(WriteMethod, unsigned char,
  kRocksdbNative, kToplingMmapWrite, kToplingFileWrite
);
struct TopFastTableOptions {
  std::vector<size_t> compessionThreshold;
  std::string indexType;
  size_t keyPrefixLen = 0;
  uint32_t fileWriteBufferSize = 8 * 1024;
  WarmupLevel warmupLevel = WarmupLevel::kValue;
  WriteMethod writeMethod = WriteMethod::kToplingFileWrite;
  bool useFilePreallocation = true;
};

class TopFastTableFactory : public TableFactory {
public:
  explicit
  TopFastTableFactory(const TopFastTableOptions&);
  ~TopFastTableFactory() override;

  const char* Name() const override;

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

  TopFastTableOptions table_options_;

  // stats
  mutable long long start_time_point_;
  mutable long long build_time_duration_; // exclude idle time
  mutable size_t num_writers_;
  mutable size_t num_readers_;
  mutable size_t sum_full_key_len_;
  mutable size_t sum_user_key_len_;
  mutable size_t sum_user_key_cnt_;
  mutable size_t sum_value_len_;
  mutable size_t sum_entry_cnt_; // of all writers(builders)
  mutable size_t sum_index_len_;
  mutable size_t sum_index_num_;
  mutable size_t sum_multi_num_;

  friend class TopFastTableBuilder;
  friend class TopFastTableReader;
};

class SingleFastTableFactory : public TopFastTableFactory {
public:
  using TopFastTableFactory::TopFastTableFactory;
  const char* Name() const final;
  using TableFactory::NewTableReader;
  Status
  NewTableReader(const ReadOptions&,
                 const TableReaderOptions&,
                 std::unique_ptr<RandomAccessFileReader>&&,
                 uint64_t file_size,
                 std::unique_ptr<TableReader>*,
                 bool prefetch_index_and_filter_in_cache) const final;

  TableBuilder* NewTableBuilder(const TableBuilderOptions&,
                                WritableFileWriter*) const final;
};

std::shared_ptr<TableFactory> NewTopFastTableFactory(const TopFastTableOptions&);

}

