#include "top_table_builder.h"
#include "top_table_reader.h" // for kTopEmptyTableKey ...
#include <rocksdb/merge_operator.h>
#include <logging/logging.h>
#include <table/meta_blocks.h>
#include <util/xxhash.h>

namespace rocksdb {


TopTableBuilderBase::TopTableBuilderBase(const TableBuilderOptions& tbo,
                                         WritableFileWriter* file)
  : ioptions_(tbo.ioptions), file_(file), range_del_block_(1)
{
  TERARK_VERIFY(nullptr != ioptions_.user_comparator);
  TERARK_VERIFY(nullptr != ioptions_.user_comparator->Name());
  TERARK_VERIFY(IsBytewiseComparator(ioptions_.user_comparator));

 #if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) < 70060
  properties_.creation_time = tbo.creation_time;
 #else
  properties_.creation_time = tbo.file_creation_time;
 #endif
  properties_.file_creation_time = tbo.file_creation_time;
  properties_.oldest_key_time = tbo.oldest_key_time;
  properties_.orig_file_number = tbo.cur_file_num;
  properties_.db_id = tbo.db_id;
  properties_.db_session_id = tbo.db_session_id;
  properties_.db_host_id = tbo.ioptions.db_host_id;
  if (!ReifyDbHostIdProperty(tbo.ioptions.env, &properties_.db_host_id).ok()) {
    INFO(tbo.ioptions.logger, "db_host_id property will not be set");
  }
  properties_.fixed_key_len = 0;
  properties_.num_data_blocks = 1;
  properties_.column_family_id = tbo.column_family_id;
  properties_.column_family_name = tbo.column_family_name;
  properties_.comparator_name = ioptions_.user_comparator->Name();
  properties_.merge_operator_name = ioptions_.merge_operator ?
    ioptions_.merge_operator->Name() : "nullptr";
  properties_.prefix_extractor_name = tbo.moptions.prefix_extractor ?
    tbo.moptions.prefix_extractor->Name() : "nullptr";

  isReverseBytewiseOrder_ = !IsForwardBytewiseComparator(ioptions_.user_comparator);

  if (tbo.int_tbl_prop_collector_factories) {
    const auto& factories = *tbo.int_tbl_prop_collector_factories;
    collectors_.resize(factories.size());
    auto cfId = (uint32_t)properties_.column_family_id;
    for (size_t i = 0; i < collectors_.size(); ++i) {
     #if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 60260
      collectors_[i].reset(factories[i]->CreateIntTblPropCollector(cfId, tbo.level_at_creation));
     #else
      collectors_[i].reset(factories[i]->CreateIntTblPropCollector(cfId));
     #endif
    }
  }

  std::string property_collectors_names = "[";
  for (size_t i = 0;
    i < ioptions_.table_properties_collector_factories.size(); ++i) {
    if (i != 0) {
      property_collectors_names += ",";
    }
    property_collectors_names +=
      ioptions_.table_properties_collector_factories[i]->Name();
  }
  property_collectors_names += "]";
  properties_.property_collectors_names = property_collectors_names;

  file_ = file;
}

std::string TopTableBuilderBase::GetFileChecksum() const {
  if (file_ != nullptr) {
    return file_->GetFileChecksum();
  } else {
    return kUnknownFileChecksum;
  }
}

const char* TopTableBuilderBase::GetFileChecksumFuncName() const {
  if (file_ != nullptr) {
    return file_->GetFileChecksumFuncName();
  } else {
    return kUnknownFileChecksumFuncName;
  }
}

bool TopTableBuilderBase::NeedCompact() const {
  for (const auto& collector : this->collectors_) {
    if (collector->NeedCompact()) {
      return true;
    }
  }
  return false;
}

void TopTableBuilderBase::WriteMeta(uint64_t magic,
    std::initializer_list<std::pair<const std::string&, BlockHandle>> blocks) {
  MetaIndexBuilder metaindexBuilder;
  for (const auto& block : blocks) {
    if (!block.second.IsNull())
      metaindexBuilder.Add(block.first, block.second);
  }
  auto log = ioptions_.info_log.get();
  {
    PropertyBlockBuilder propBlockBuilder;
    propBlockBuilder.AddTableProperty(properties_);
    for (auto& collector : collectors_) {
      Status s = collector->Finish(&properties_.user_collected_properties);
      if (s.ok()) {
        for (const auto& prop : collector->GetReadableProperties()) {
          properties_.readable_properties.insert(prop);
        }
        propBlockBuilder.Add(properties_.user_collected_properties);
      }
      else
        LogPropertiesCollectionError(log, "Finish", collector->Name());
    }
    metaindexBuilder.Add(kPropertiesBlock,
                        WriteBlock(propBlockBuilder.Finish(), file_, &offset_));
  }
  if (!range_del_block_.empty()) {
    metaindexBuilder.Add(kRangeDelBlock,
                        WriteBlock(range_del_block_.Finish(), file_, &offset_));
  }
  uint32_t version = 1;
  WriteFileFooter(magic, version, metaindexBuilder.Finish(), file_, &offset_);
}

void TopTableBuilderBase::FinishAsEmptyTable() {
  auto log = ioptions_.info_log.get();
  INFO(log, "TopTableBuilderBase::EmptyFinish():this=%12p\n", this);
  ROCKSDB_VERIFY_EZ(offset_);
  // 1. EmptyTable can have range_del block
  // 2. Magic of EmptyTable is kTopEmptyTableMagicNumber
  BlockHandle emptyBH = WriteBlock(Slice("Empty"), file_, &offset_);
  WriteMeta(kTopEmptyTableMagicNumber, {{kTopEmptyTableKey, emptyBH}});
}

static XXH32_state_t* XXH32_init(unsigned int seed) {
  XXH32_state_t* st = XXH32_createState();
  XXH32_reset(st, seed);
  return st;
}
static
void WriteBlockTrailer(const Slice& block, WritableFileWriter* file, uint64_t* offset) {
#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 60280
  constexpr size_t kBlockTrailerSize = 5; // BlockBasedTable::kBlockTrailerSize
#endif
  char trailer[kBlockTrailerSize];
  trailer[0] = kxxHash;
  auto xxh = XXH32_init(0);
  XXH32_update(xxh, block.data(), static_cast<uint32_t>(block.size()));
  XXH32_update(xxh, trailer, 1);  // Extend  to cover block checksum type
  EncodeFixed32(trailer + 1, XXH32_digest(xxh));
  WriteBlock(Slice(trailer, kBlockTrailerSize), file, offset);
}

void WriteFileFooter(uint64_t magic, uint32_t format_version, const Slice& meta,
                     WritableFileWriter* file, uint64_t* offset) {
  BlockHandle metaHandle = WriteBlock(meta, file, offset);
  WriteBlockTrailer(meta, file, offset);
#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 60280
  // rocksdb-6.28 use format_version to check legacy format(0 means legacy),
  // but older rocksdb use IsLegacyFooterFormat(magic) to check legacy format.
  // so we set format_version >= 1 to make both old and new rocksdb code happy.
  ROCKSDB_VERIFY_GE(format_version, 1);
  FooterBuilder footer;
  footer.Build(magic, format_version, *offset, kxxHash, metaHandle);
  WriteBlock(footer.GetSlice(), file, offset);
#else
  Footer footer(magic, 0/*version*/);
  footer.set_metaindex_handle(metaHandle);
  footer.set_index_handle(BlockHandle::NullBlockHandle());
  footer.set_checksum(kxxHash);
  std::string footer_encoding;
  footer.EncodeTo(&footer_encoding);
  WriteBlock(footer_encoding, file, offset);
#endif
}

terark::profiling g_pf;
const uint64_t kTopEmptyTableMagicNumber = 0x1122334455667788;
const std::string kTopEmptyTableKey    = "ThisIsAnEmptyTable";

} // namespace rocksdb
