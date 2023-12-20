#include "top_table_common.h"
#include <terark/io/DataIO.hpp>
#include <terark/io/MemStream.hpp>
#include <terark/util/autofree.hpp>
#include <terark/util/throw.hpp>
#include <stdlib.h>
#include <ctime>
#ifdef _MSC_VER
# include <io.h>
#else
# include <sys/types.h>
# include <sys/stat.h>
# include <fcntl.h>
# include <cxxabi.h>
#endif
#include <boost/core/demangle.hpp>

namespace rocksdb {

void AutoDeleteFile::Delete() {
  if (!fpath.empty()) {
    ::remove(fpath.c_str());
    fpath.clear();
  }
}
AutoDeleteFile::~AutoDeleteFile() {
  if (!fpath.empty()) {
    ::remove(fpath.c_str());
  }
}

TempFileDeleteOnClose::~TempFileDeleteOnClose() {
  if (fp)
    this->close();
  if (!path.empty())
    ::remove(path.c_str());
}

/// this->path is temporary filename template such as: /some/dir/tmpXXXXXX
void TempFileDeleteOnClose::open_temp() {
  if (!terark::fstring(path).endsWith("XXXXXX")) {
    THROW_STD(invalid_argument,
        "ERROR: path = \"%s\", must ends with \"XXXXXX\"", path.c_str());
  }
#if _MSC_VER
  if (int err = _mktemp_s(&path[0], path.size() + 1)) {
    THROW_STD(invalid_argument, "ERROR: _mktemp_s(%s) = %s"
        , path.c_str(), strerror(err));
  }
  this->open();
#else
  int fd = mkstemp(&path[0]);
  if (fd < 0) {
    int err = errno;
    THROW_STD(invalid_argument, "ERROR: mkstemp(%s) = %s"
        , path.c_str(), strerror(err));
  }
  this->dopen(fd);
#endif
}
void TempFileDeleteOnClose::open(const char* mode) {
  fp.open(path.c_str(), mode);
  fp.disbuf();
  writer.attach(&fp);
}
void TempFileDeleteOnClose::dopen(int fd) {
  fp.dopen(fd, "wb+");
  fp.disbuf();
  writer.attach(&fp);
}
void TempFileDeleteOnClose::close() {
  assert(nullptr != fp);
  writer.resetbuf();
  fp.close();
  if (!path.empty())
    ::remove(path.c_str());
}
void TempFileDeleteOnClose::complete_write() {
  writer.flush_buffer();
  fp.rewind();
}

/*
std::string GetFileNameByFD(intptr_t fd) {
  std::string path;
  path.resize(PATH_MAX);
  intptr_t linklen = 0;
#ifdef OS_MACOSX
  if (fcntl(fd, F_GETPATH, &path[0]) != -1) {
    linklen = strlen(&path[0]);
  }
  else {
    fprintf(stderr, "ERROR: F_GETPATH(%zd) = %s\n", fd, strerror(errno));
  }
#elif defined(OS_LINUX)
  char fdname[32];
  snprintf(fdname, sizeof(fdname), "/proc/self/fd/%zd", fd);
  intptr_t linklen = readlink(fdname, &path[0], path.size());
  if (linklen > 0) {
  }
  else {
    fprintf(stderr, "ERROR: readlink(%s) = %s\n", fdname, strerror(errno));
    linklen = 0;
  }
#elif defined(_MSC_VER)
  // TODO:
#endif
  path.resize(linklen);
  auto beg = path.cbegin();
  auto pos = path.cend();
  while (pos > beg && '/' != *--pos) {}
  path.assign(&*pos, path.end() - pos);
  path.shrink_to_fit();
  return path;
}
*/

size_t TableMultiPartInfo::calc_size(size_t prefixLen, size_t partCount) {
  BOOST_STATIC_ASSERT(sizeof(KeyValueOffset) % 16 == 0);
  return terark::align_up(8
      + (partCount + 1) * sizeof(KeyValueOffset)
      + (partCount + 0) * prefixLen, 16);
}

void TableMultiPartInfo::Init(size_t prefixLen, size_t partCount) {
  offset_.ensure_capacity(partCount + 1);
  prefixSet_.m_fixlen = uint32_t(prefixLen);
  prefixSet_.m_strpool.ensure_capacity(prefixLen * partCount);
}

valvec<byte_t> TableMultiPartInfo::dump() {
  TERARK_VERIFY_EQ(prefixSet_.size() + 1, offset_.size());
  size_t size = calc_size(prefixSet_.m_fixlen, offset_.size());
  LittleEndianDataOutput<terark::AutoGrownMemIO> buf(size);
  buf << uint32_t(prefixSet_.size()); // partCount
  buf << uint32_t(prefixSet_.m_fixlen);
  buf.write(offset_.data(), offset_.used_mem_size());
  buf.write(prefixSet_.data(), prefixSet_.str_size());
  valvec<byte_t> ret;
  ret.risk_set_data(buf.begin());
  ret.risk_set_size(buf.tell());
  ret.risk_set_capacity(buf.capacity());
  buf.risk_release_ownership();
  return ret;
}

bool TableMultiPartInfo::risk_set_memory(const void* p, size_t s) {
  offset_.clear();
  prefixSet_.clear();
  if (s < 16) {
    return false;
  }
  TERARK_ASSERT_AL(size_t(p), 8);
  LittleEndianDataInput<terark::MemIO> dio(p, s);
  size_t partCount = dio.load_as<uint32_t>();
  uint32_t fixlen = dio.load_as<uint32_t>();
  offset_.risk_set_data((KeyValueOffset*)dio.current(), partCount + 1);
  dio.skip(offset_.used_mem_size());
  TERARK_ASSERT_AL(size_t(dio.current()), 8);
  prefixSet_.m_strpool.risk_set_data(dio.current(), fixlen * partCount);
  prefixSet_.m_fixlen = fixlen;
  prefixSet_.m_size = partCount;
  prefixSet_.optimize_func();
  return true;
}

void TableMultiPartInfo::risk_release_ownership() {
  offset_.risk_release_ownership();
  prefixSet_.risk_release_ownership();
}

} // namespace rocksdb

