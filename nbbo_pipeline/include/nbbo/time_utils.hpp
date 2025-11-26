#pragma once
#include <cstdint>
#include <string>

namespace nbbo {

// Provides utils for working with nbbo timestamps
// - Extracts calendar fields (year, month, day, time)
// - Computes ms since midnight
// - Increments timestamps by 1 ms
// - Extracts day indicators used when data processing

inline uint32_t ymd(uint64_t ts) {
  return static_cast<uint32_t>(ts / 1000000000ULL);
}
inline int hh(uint64_t ts) {
  return static_cast<int>((ts / 10000000ULL) % 100ULL);
}
inline int mm(uint64_t ts) {
  return static_cast<int>((ts / 100000ULL) % 100ULL);
}
inline int ss(uint64_t ts) { return static_cast<int>((ts / 1000ULL) % 100ULL); }
inline int mmm(uint64_t ts) { return static_cast<int>(ts % 1000ULL); }

inline bool same_day(uint64_t a, uint64_t b) { return ymd(a) == ymd(b); }

inline int ms_since_midnight(uint64_t ts) {
  return ((hh(ts) * 60 + mm(ts)) * 60 + ss(ts)) * 1000 + mmm(ts);
}

inline uint64_t inc_ms(uint64_t ts) {
  int H = hh(ts), M = mm(ts), S = ss(ts), MS = mmm(ts);
  int Y = static_cast<int>(ts / 1000000000000ULL);
  int Mon = static_cast<int>((ts / 10000000000ULL) % 100ULL);
  int D = static_cast<int>((ts / 100000000ULL) % 100ULL);
  if (++MS == 1000) {
    MS = 0;
    if (++S == 60) {
      S = 0;
      if (++M == 60) {
        M = 0;
        ++H;
      }
    }
  }
  return static_cast<uint64_t>(Y) * 1000000000000ULL +
         static_cast<uint64_t>(Mon) * 10000000000ULL +
         static_cast<uint64_t>(D) * 100000000ULL +
         static_cast<uint64_t>(H) * 10000000ULL +
         static_cast<uint64_t>(M) * 100000ULL +
         static_cast<uint64_t>(S) * 1000ULL + static_cast<uint64_t>(MS);
}

inline int year_from_ts(uint64_t ts) {
  return static_cast<int>(ts / 10000000000000ULL);
}

inline uint32_t day_from_ts(uint64_t ts) {
  return static_cast<uint32_t>(ts / 1000000000ULL);
}

inline std::string day_to_string(uint32_t d) {
  uint32_t y = d / 10000U;
  uint32_t m = (d / 100U) % 100U;
  uint32_t dd = d % 100U;
  char buf[16];
  std::snprintf(buf, sizeof(buf), "%04u-%02u-%02u", y, m, dd);
  return std::string(buf);
}
}  // namespace nbbo
