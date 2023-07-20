//
// Created by leipeng on 2020/8/23.
//

#include "top_fast_table.h"
#include <table/table_builder.h>

#include "top_table_common.h"
#include "top_fast_table_internal.h"
#include <topling/builtin_table_factory.h>
#include <topling/side_plugin_factory.h>
#include <terark/num_to_str.hpp>

const char* git_version_hash_info_topling_sst();
const char* git_version_hash_info_zbs();

namespace ROCKSDB_NAMESPACE {

const char* TopFastTableFactory::Name() const {
  return "ToplingFastTable";
}
const char* SingleFastTableFactory::Name() const {
  return "SingleFastTable";
}

TopFastTableFactory::TopFastTableFactory(const TopFastTableOptions& tfto) {
  table_options_ = tfto;
  start_time_point_ = g_pf.now();
  build_time_duration_ = 0;
  num_writers_ = 0;
  num_readers_ = 0;
  sum_full_key_len_ = 0;
  sum_user_key_cnt_ = 0;
  sum_user_key_len_ = 0;
  sum_value_len_ = 0;
  sum_entry_cnt_ = 0;
  sum_index_len_ = 0;
  sum_index_num_ = 0;
  sum_multi_num_ = 0;
}

TopFastTableFactory::~TopFastTableFactory() { // NOLINT
}

std::string TopFastTableFactory::GetPrintableOptions() const {
  return std::string();
}

Status
TopFastTableFactory::ValidateOptions(const DBOptions& db_opts,
                                        const ColumnFamilyOptions& cf_opts)
const {
  if (!IsBytewiseComparator(cf_opts.comparator)) {
    return Status::InvalidArgument(ROCKSDB_FUNC, "comparator is not bytewise");
  }
  TERARK_VERIFY_EZ(cf_opts.comparator->timestamp_size());
  return Status::OK();
}

std::shared_ptr<TableFactory>
NewTopFastTableFactory(const TopFastTableOptions& options) {
  return std::make_shared<TopFastTableFactory>(options);
}

/////////////////////////////////////////////////////////////////////////////

struct TopFastTableOptions_Json : TopFastTableOptions {
  TopFastTableOptions_Json(const json& js, const SidePluginRepo& repo) {
    Update(js, repo);
  }
  void Update(const json& js, const SidePluginRepo&) {
    //ROCKSDB_JSON_OPT_PROP(js, compressionThreshold);
    ROCKSDB_JSON_OPT_PROP(js, indexType);
    ROCKSDB_JSON_OPT_PROP(js, keyPrefixLen);
    ROCKSDB_JSON_OPT_SIZE(js, fileWriteBufferSize);
    ROCKSDB_JSON_OPT_ENUM(js, warmupLevel);
    ROCKSDB_JSON_OPT_ENUM(js, writeMethod);
    ROCKSDB_JSON_OPT_PROP(js, accurateKeyAnchorsSize);
    ROCKSDB_JSON_OPT_SIZE(js, keyAnchorSizeUnit);
    ROCKSDB_JSON_OPT_PROP(js, useFilePreallocation);
    ROCKSDB_JSON_OPT_PROP(js, debugLevel);
  }
  void ToJson(json& djs, const json& dump_options, const SidePluginRepo&) const {
    //ROCKSDB_JSON_SET_PROP(djs, compressionThreshold);
    ROCKSDB_JSON_SET_PROP(djs, indexType);
    ROCKSDB_JSON_SET_PROP(djs, keyPrefixLen);
    ROCKSDB_JSON_SET_SIZE(djs, fileWriteBufferSize);
    ROCKSDB_JSON_SET_ENUM(djs, warmupLevel);
    ROCKSDB_JSON_SET_ENUM(djs, writeMethod);
    ROCKSDB_JSON_SET_PROP(djs, accurateKeyAnchorsSize);
    ROCKSDB_JSON_SET_SIZE(djs, keyAnchorSizeUnit);
    ROCKSDB_JSON_SET_PROP(djs, useFilePreallocation);
    ROCKSDB_JSON_SET_PROP(djs, debugLevel);
  }
};
static
std::shared_ptr<TableFactory>
JS_NewTopFastTableFactory(const json& js, const SidePluginRepo& repo) {
  TopFastTableOptions_Json options(js, repo);
  return NewTopFastTableFactory(options);
}
ROCKSDB_FACTORY_REG("ToplingFastTable", JS_NewTopFastTableFactory);
ROCKSDB_RegTableFactoryMagicNumber(kTopFastTableMagic, "ToplingFastTable");

static
std::shared_ptr<TableFactory>
JS_NewSingleFastTableFactory(const json& js, const SidePluginRepo& repo) {
  TopFastTableOptions_Json options(js, repo);
  return std::make_shared<SingleFastTableFactory>(options);
}
ROCKSDB_FACTORY_REG("SingleFastTable", JS_NewSingleFastTableFactory);
ROCKSDB_RegTableFactoryMagicNumber(kSingleFastTableMagic, "SingleFastTable");

#define GITHUB_TOPLING_SST "https://github.com/topling/topling-sst"
// now GITHUB_TOPLING_ZIP is defined in toplingdb/Makefile
// #define GITHUB_TOPLING_ZIP "https://github.com/rockeet/topling-core"
void JS_TopTable_AddVersion(json& ver, bool html) {
  if (html) {
    std::string topling_sst = HtmlEscapeMin(strstr(git_version_hash_info_topling_sst(), "commit ") + strlen("commit "));
    std::string topling_zip = HtmlEscapeMin(strchr(git_version_hash_info_zbs(), ':') + 1);
    auto headstr = [](const std::string& s, auto pos) {
      return terark::fstring(s.data(), pos - s.begin());
    };
    auto tailstr = [](const std::string& s, auto pos) {
      return terark::fstring(&*pos, s.end() - pos);
    };
    auto topling_sst_sha_end = std::find_if(topling_sst.begin(), topling_sst.end(), &isspace);
    auto topling_zip_sha_end = std::find_if(topling_zip.begin(), topling_zip.end(), &isspace);
    terark::string_appender<> oss_sst, oss_zip;
    oss_sst|"<pre>"
           |"<a href='"|GITHUB_TOPLING_SST|"/commit/"
           |headstr(topling_sst, topling_sst_sha_end)|"'>"
           |headstr(topling_sst, topling_sst_sha_end)|"</a>"
           |tailstr(topling_sst, topling_sst_sha_end)
           |"</pre>";
    oss_zip|"<pre>"
           |"<a href='"|GITHUB_TOPLING_ZIP|"/commit/"
           |headstr(topling_zip, topling_zip_sha_end)|"'>"
           |headstr(topling_zip, topling_zip_sha_end)|"</a>"
           |tailstr(topling_zip, topling_zip_sha_end)
           |"</pre>";
    ver["topling-sst"] = static_cast<std::string&&>(oss_sst);
    ver["topling-zip"] = static_cast<std::string&&>(oss_zip);
  } else {
    ver["topling-sst"] = git_version_hash_info_topling_sst();
    ver["topling-zip"] = git_version_hash_info_zbs(); // zbs depends on core
  }
}
struct TopFastTableFactory_Json : TopFastTableFactory {
  void ToJson(json& djs, bool html) const {
    djs["num_writers"] = num_writers_;
    djs["num_readers"] = num_readers_;

    long long t8 = g_pf.now();
    double td = g_pf.uf(start_time_point_, t8);
    size_t sumlen = sum_full_key_len_ + sum_value_len_;
    size_t mem_s = sizeof(TopFastIndexEntry) * (sum_user_key_cnt_ - sum_multi_num_);
    size_t mem_m = sizeof(TopFastIndexEntry) * (sum_multi_num_)
                 + sizeof(uint32_t) * (sum_multi_num_ * 2 + sum_entry_cnt_ - sum_user_key_cnt_)
                 + sizeof(uint64_t) * (sum_entry_cnt_ - sum_user_key_cnt_) // outline seqnum
                 ;
    char buf[64];
#define ToStr(...) json(std::string(buf, snprintf(buf, sizeof(buf), __VA_ARGS__)))

    djs["Statistics"] = json::object({
      { "sum_full_key_len", ToStr("%.3f GB", sum_full_key_len_/1e9) },
      { "sum_user_key_len", ToStr("%.3f GB", sum_user_key_len_/1e9) },
      { "sum_user_key_cnt", sum_user_key_cnt_ },
      { "sum_value_len", ToStr("%.3f GB", sum_value_len_/1e9) },
      { "sum_entry_cnt", sum_entry_cnt_ },
      { "sum_index_len", ToStr("%.3f GB", sum_index_len_/1e9) },
      { "sum_index_num", sum_index_num_},
      { "avg_index_len", ToStr("%.3f MB", sum_index_len_/1e6/sum_index_num_) },
      { "raw:avg_uval" , ToStr("%.3f", sum_value_len_/1.0/sum_user_key_cnt_) },
      { "raw:avg_ikey", ToStr("%.3f", sum_full_key_len_/1.0/sum_entry_cnt_) },
      { "raw:avg_ukey", ToStr("%.3f", sum_user_key_len_/1.0/sum_user_key_cnt_) },
      { "zip:avg_ikey", ToStr("%.3f", sum_index_len_/1.0/sum_user_key_cnt_) },
      { "zip:avg_ukey", ToStr("%.3f", (sum_index_len_ - mem_m - mem_s)/1.0/sum_user_key_cnt_) },
      { "zip:avg_node", ToStr("%.3f", (mem_m + mem_s)/1.0/sum_entry_cnt_) },
      { "index:zip/raw", ToStr("%.3f", sum_index_len_/1.0/sum_user_key_len_) },
      { "raw:key/all", ToStr("%.3f", sum_full_key_len_/1.0/sumlen) },
      { "raw:val/all", ToStr("%.3f", sum_value_len_/1.0/sumlen) },
      { "zip:key/all", ToStr("%.3f", sum_index_len_/1.0/(sum_index_len_ + sum_value_len_)) },
      { "zip:val/all", ToStr("%.3f", sum_value_len_/1.0/(sum_index_len_ + sum_value_len_)) },
      { "write_speed_all.+seq", ToStr("%.3f MB/s", (sumlen) / td) },
      { "write_speed_all.-seq", ToStr("%.3f MB/s", (sumlen - sum_user_key_cnt_ * 8) / td) },
    });
    JS_TopTable_AddVersion(djs, html);
    JS_ToplingDB_AddVersion(djs, html);
  }
};

struct TopFastTableFactory_Manip : PluginManipFunc<TableFactory> {
  void Update(TableFactory* p, const json&, const json& js,
              const SidePluginRepo& repo) const final {
    if (auto t = dynamic_cast<TopFastTableFactory*>(p)) {
      auto o = (TopFastTableOptions_Json*)(&t->table_options_);
      o->Update(js, repo);
      return;
    }
    std::string name = p->Name();
    THROW_InvalidArgument("Is not ToplingFastTable, but is: " + name);
  }
  std::string ToString(const TableFactory& fac, const json& dump_options,
                       const SidePluginRepo& repo) const final {
    if (auto t = dynamic_cast<const TopFastTableFactory*>(&fac)) {
      // damn! GetOptions is not const, use 'm' for mutable ptr
      auto o = (TopFastTableOptions_Json*)(&t->table_options_);
      json djs;
      djs["document"] = (dynamic_cast<const SingleFastTableFactory*>(t))
        ? "<a href='https://github.com/topling/sideplugin-wiki-en/wiki/SingleFastTable'>Document(English)</a> | "
          "<a href='https://github.com/topling/rockside/wiki/SingleFastTable'>文档（中文）</a>"
        : "<a href='https://github.com/topling/sideplugin-wiki-en/wiki/ToplingFastTable'>Document(English)</a> | "
          "<a href='https://github.com/topling/rockside/wiki/ToplingFastTable'>文档（中文）</a>"
        ;
      o->ToJson(djs, dump_options, repo);
      bool html = JsonSmartBool(dump_options, "html", true);
      static_cast<const TopFastTableFactory_Json*>(t)->ToJson(djs, html);//NOLINT
      return JsonToString(djs, dump_options);
    }
    std::string name = fac.Name();
    THROW_InvalidArgument("Is not ToplingFastTable, but is: " + name);
  }
};
ROCKSDB_REG_PluginManip("ToplingFastTable", TopFastTableFactory_Manip);
ROCKSDB_REG_PluginManip("SingleFastTable", TopFastTableFactory_Manip);

} // namespace ROCKSDB_NAMESPACE
