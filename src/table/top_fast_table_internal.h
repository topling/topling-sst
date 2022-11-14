//
// Created by leipeng on 2020/8/26.
//

#pragma once

#include <string>
#include <terark/stdtypes.hpp>
#include <terark/valvec.hpp>
#include <rocksdb/rocksdb_namespace.h>

namespace ROCKSDB_NAMESPACE {

#pragma pack(push,4)
  // for both TopFastTableBuilder and TopFastTableReader
  struct TopFastIndexEntry {
    // seqvt: SequenceNumber and ValueType
    uint64_t seqvt; // value zip bit on highest bit of ValueType
    uint32_t valuePos;
    uint32_t valueLen : 31;
    uint32_t valueMul :  1;
  };
#pragma pack(pop)

extern const uint64_t kTopFastTableMagic;
extern const uint64_t kSingleFastTableMagic;
extern const std::string kCSPPIndex;

//inline uint64_t GetSeqNum(uint64_t seqvt) { return seqvt >> 8; }

struct ExtractSeqNum {
  uint64_t operator()(const uint64_t& xr) const {
    auto seqvt = unaligned_load<uint64_t>(&xr);
    return seqvt >> 8u;
  }
};

inline size_t lower_bound_seq(const uint64_t* seqArr, size_t n, uint64_t seq) {
  return terark::lower_bound_ex_n(seqArr, 0, n, seq,
                          ExtractSeqNum(), std::greater<uint64_t>()); //NOLINT
}

} // ROCKSDB_NAMESPACE
