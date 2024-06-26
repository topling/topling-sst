#pragma once

#include <terark/io/DataIO.hpp>
#include <terark/io/FileStream.hpp>
#include <terark/io/StreamBuffer.hpp>
#include <terark/util/profiling.hpp>
#include <terark/util/sortable_strvec.hpp>

#include <inttypes.h>
#include <rocksdb/preproc.h>
#include <rocksdb/slice.h>
#include <rocksdb/version.h>
#include <logging/logging.h>

namespace rocksdb {

using terark::StrDateTimeNow;
#define LOG_POS_ARGS rocksdb::StrDateTimeNow(), RocksLogShorterFileName(__FILE__), __LINE__
#define STD_INFO(format, ...) fprintf(stderr, "%s INFO %s:%d: " format "\n", LOG_POS_ARGS, ##__VA_ARGS__)
#define STD_WARN(format, ...) fprintf(stderr, "%s WARN %s:%d: " format "\n", LOG_POS_ARGS, ##__VA_ARGS__)

#undef INFO
#undef WARN
#if defined(NDEBUG) || 1
# define INFO ROCKS_LOG_INFO
# define WARN ROCKS_LOG_WARN
# define WARN_EXCEPT(logger, format, ...) \
    WARN(logger, format, ##__VA_ARGS__); \
    LogFlush(logger); \
    STD_WARN(format, ##__VA_ARGS__)
#else
# define INFO(logger, format, ...) STD_INFO(format, ##__VA_ARGS__)
# define WARN(logger, format, ...) STD_WARN(format, ##__VA_ARGS__)
# define WARN_EXCEPT WARN
#endif

#define VERIFY_STATUS_OK(s) TERARK_VERIFY_F(s.ok(), "%s", s.ToString().c_str())

using std::string;
using std::unique_ptr;

using terark::byte_t;
using terark::fstring;
using terark::valvec;
using terark::valvec_no_init;
using terark::valvec_reserve;

using terark::FileStream;
using terark::InputBuffer;
using terark::OutputBuffer;
using terark::LittleEndianDataInput;
using terark::LittleEndianDataOutput;

extern terark::profiling g_pf;

template<class T>
inline unique_ptr<T> UniquePtrOf(T* p) { return unique_ptr<T>(p); }

#if (ROCKSDB_MAJOR * 10000 + ROCKSDB_MINOR * 10 + ROCKSDB_PATCH) >= 60280
const std::string kPropertiesBlock = "rocksdb.properties";
const std::string kRangeDelBlock = "rocksdb.range_del";
#endif

inline uint64_t ReadBigEndianUint64(const void* beg, size_t len) {
  ROCKSDB_ASSUME(len <= 8);
  union {
    byte_t bytes[8];
    uint64_t value;
  } c;
  c.value = 0;  // this is fix for gcc-4.8 union init bug
  memcpy(c.bytes + (8 - len), beg, len);
  return NATIVE_OF_BIG_ENDIAN(c.value);
}
inline uint64_t ReadBigEndianUint64(const byte_t* beg, const byte_t* end) {
  assert(end - beg <= 8);
  return ReadBigEndianUint64(beg, end-beg);
}
inline uint64_t ReadBigEndianUint64(fstring data) {
  assert(data.size() <= 8);
  return ReadBigEndianUint64((const byte_t*)data.data(), data.size());
}
inline uint64_t SafeReadBigEndianUint64(const void* beg, size_t len) {
  return ReadBigEndianUint64(beg, len < 8 ? len : 8);
}
inline uint64_t SafeReadBigEndianUint64(fstring data) {
  return SafeReadBigEndianUint64(data.p, data.n);
}

inline
uint64_t ReadBigEndianUint64Aligned(const byte_t* beg, size_t len) {
  assert(8 == len); TERARK_UNUSED_VAR(len);
  return NATIVE_OF_BIG_ENDIAN(*(const uint64_t*)beg);
}

inline void SaveAsBigEndianUint64(byte_t* beg, size_t len, uint64_t value) {
  assert(len <= 8);
  ROCKSDB_ASSUME(len <= 8);
  union {
    byte_t bytes[8];
    uint64_t value;
  } c;
  c.value = BIG_ENDIAN_OF(value);
  memcpy(beg, c.bytes + (8 - len), len);
}

template<class T>
inline void correct_minmax(T& minVal, T& maxVal) {
  if (maxVal < minVal) {
    using namespace std;
    swap(maxVal, minVal);
  }
}

template<class T>
T abs_diff(const T& x, const T& y) {
  if (x < y)
    return y - x;
  else
    return x - y;
}

std::string demangle(const char* name);

template<class T>
inline std::string ClassName() {
  return demangle(typeid(T).name());
}
template<class T>
inline std::string ClassName(const T& x) {
  return demangle(typeid(x).name());
}

class AutoDeleteFile {
public:
  std::string fpath;
  operator fstring() const { return fpath; }
  void Delete();
  ~AutoDeleteFile();
};

class TempFileDeleteOnClose {
public:
  std::string path;
  FileStream  fp;
  NativeDataOutput<OutputBuffer> writer;
  ~TempFileDeleteOnClose();
  void open_temp();
  void open(const char* mode = "wb+");
  void dopen(int fd);
  void close();
  void complete_write();
};

template<size_t Align, class Writer>
void Padzero(const Writer& write, size_t offset) {
  static const char zeros[Align] = { 0 };
  if (offset % Align) {
    write(zeros, Align - offset % Align);
  }
}

void MlockBytes(const void* addr, size_t len);
template<class T>
inline void MlockMem(const T* addr, size_t len) {
  MlockBytes(addr, sizeof(T)*len);
}
template<class Vec>
void MlockMem(const Vec& uv) {
  MlockBytes(uv.data(), uv.size() * sizeof(uv.data()[0]));
}

void MmapWarmUpBytes(const void* addr, size_t len);
template<class T>
inline void MmapWarmUp(const T* addr, size_t len) {
  MmapWarmUpBytes(addr, sizeof(T)*len);
}
template<class Vec>
void MmapWarmUp(const Vec& uv) {
  MmapWarmUpBytes(uv.data(), uv.size() * sizeof(uv.data()[0]));
}

void MmapColdizeBytes(const void* addr, size_t len);
template<class T>
inline void MmapColdize(const T* addr, size_t len) {
  MmapColdizeBytes(addr, sizeof(T)*len);
}
template<class Vec>
inline void MmapColdize(const Vec& uv) {
  MmapColdizeBytes(uv.data(), sizeof(uv.data()[0]) * uv.size());
}

void MmapAdvRnd(const void* addr, size_t len);
template<class Vec>
inline void MmapAdvRnd(const Vec& v) {
  MmapAdvRnd(v.data(), sizeof(v.data()[0]) * v.size());
}

void MmapAdvSeq(const void* addr, size_t len);
template<class Vec>
inline void MmapAdvSeq(const Vec& v) {
  MmapAdvSeq(v.data(), sizeof(v.data()[0]) * v.size());
}

terark_forceinline
void TryWarmupZeroCopy(const valvec<byte_t>& buf, size_t min_prefault_pages) {
#ifdef TOPLINGDB_WARMUP_ZERO_COPY
  if (buf.capacity() == 0 && buf.size() != 0) {
    size_t lo = terark::pow2_align_down(size_t(buf.data()), 4096);
    size_t hi = terark::pow2_align_up(size_t(buf.end()), 4096);
    size_t len = hi - lo;
    if (UNLIKELY(len >= min_prefault_pages * 4096)) {
      MmapWarmUpBytes((void*)lo, len);
    }
  }
#endif
}

// compatible to DFA_MmapHeaderBase, so UintIndex/CompositeIndex header
// are compabitle to NLT index header
struct ToplingIndexHeader {
  uint8_t   magic_len;
  char      magic[19];
  char      class_name[60];

  uint32_t  reserved_80_4;
  uint32_t  header_size; // same offset of DFA_MmapHeaderBase::header_size
  uint32_t  version;
  uint32_t  reserved_92_4;

  uint64_t  file_size; // same offset of DFA_MmapHeaderBase::file_size
  uint64_t  reserved_102_24;
};

struct KeyRankCacheEntry { // for ApproximateOffsetOf
  uint32_t num;
  uint32_t cache_bytes;
  uint32_t fixed_suffix_len; // == 0 means varlen
  uint32_t cache_prefix_len;
  byte_t   cache_prefix_data[48];
  fstring  cache_prefix() const noexcept {
    return {cache_prefix_data, cache_prefix_len};
  }
};
static_assert(sizeof(KeyRankCacheEntry) == 64);

struct TableMultiPartInfo {
  struct KeyValueOffset {
    size_t offset;
    // {key,value,type} means corresponding length, not offset
    size_t key;
    size_t value;
    size_t type        : 56;
    size_t tag_rs_kind :  8;
    union {
      size_t type_histogram[4];
      struct {
        size_t rs_bytes; ///< there is only one rank-select
        size_t tag_bytes;
        size_t tag_num   : 48;  ///< non-zero tag num
        size_t vtr_num   :  8;
        size_t seq_width :  8;
        size_t min_seq;
      };
    };
  };
  valvec<KeyValueOffset> offset_;
  terark::FixedLenStrVec prefixSet_;
  const KeyRankCacheEntry* krceVec_ = nullptr;

  static size_t calc_size(size_t prefixLen, size_t partCount);
  void Init(size_t prefixLen, size_t partCount);
  valvec<byte_t> dump();
  bool risk_set_memory(const Slice& d) { return risk_set_memory(d.data_, d.size_); }
  bool risk_set_memory(const void*, size_t);
  void risk_release_ownership();
};

} // namespace rocksdb
