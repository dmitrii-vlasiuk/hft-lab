#pragma once

#include <cstdint>
#include <string>
#include <cstdio>  // for std::snprintf

namespace nbbo {

// Utilities for working with NBBO-style timestamps.
//
// Convention:
//   ts is an integer-encoded timestamp of the form
//     YYYYMMDDHHMMSSmmm
//   stored in a uint64_t. All helpers below assume this layout.
//
// This header provides:
//   - Extraction of calendar fields (year-month-day, hour, minute, second, ms)
//   - Same-day checks
//   - Milliseconds-since-midnight computation
//   - Incrementing a timestamp by 1 ms (without date rollover handling beyond the day)
//   - Conversion between day integers (YYYYMMDD) and strings ("YYYY-MM-DD")

// Extract YYYYMMDD as an integer from the full timestamp.
inline uint32_t ymd(uint64_t ts) {
  return static_cast<uint32_t>(ts / 1000000000ULL);
}

// Extract hour (HH, 0–23).
inline int hh(uint64_t ts) {
  return static_cast<int>((ts / 10000000ULL) % 100ULL);
}

// Extract minute (MM, 0–59).
inline int mm(uint64_t ts) {
  return static_cast<int>((ts / 100000ULL) % 100ULL);
}

// Extract second (SS, 0–59).
inline int ss(uint64_t ts) {
  return static_cast<int>((ts / 1000ULL) % 100ULL);
}

// Extract millisecond (mmm, 0–999).
inline int mmm(uint64_t ts) {
  return static_cast<int>(ts % 1000ULL);
}

// True if two timestamps fall on the same calendar day (YYYYMMDD).
inline bool same_day(uint64_t a, uint64_t b) {
  return ymd(a) == ymd(b);
}

// Milliseconds since midnight, using the HH:MM:SS.mmm components.
inline int ms_since_midnight(uint64_t ts) {
  return ((hh(ts) * 60 + mm(ts)) * 60 + ss(ts)) * 1000 + mmm(ts);
}

// Increment timestamp by 1 ms, keeping the YYYYMMDD date fields consistent
// with the existing representation. This does NOT do full calendar arithmetic
// (e.g. month length or leap year checks); it assumes the caller stays within
// a valid intraday range.
inline uint64_t inc_ms(uint64_t ts) {
  int H   = hh(ts);
  int M   = mm(ts);
  int S   = ss(ts);
  int MS  = mmm(ts);

  int Y   = static_cast<int>(ts / 1000000000000ULL);
  int Mon = static_cast<int>((ts / 10000000000ULL) % 100ULL);
  int D   = static_cast<int>((ts / 100000000ULL) % 100ULL);

  if (++MS == 1000) {
    MS = 0;
    if (++S == 60) {
      S = 0;
      if (++M == 60) {
        M = 0;
        ++H;
        // Note: if H reaches 24, behavior is up to the caller; this function
        // does not roll to the next day/month/year.
      }
    }
  }

  return static_cast<uint64_t>(Y)   * 1000000000000ULL +
         static_cast<uint64_t>(Mon) * 10000000000ULL  +
         static_cast<uint64_t>(D)   * 100000000ULL    +
         static_cast<uint64_t>(H)   * 10000000ULL     +
         static_cast<uint64_t>(M)   * 100000ULL       +
         static_cast<uint64_t>(S)   * 1000ULL         +
         static_cast<uint64_t>(MS);
}

// Extract the 4-digit year (YYYY) from the timestamp.
inline int year_from_ts(uint64_t ts) {
  return static_cast<int>(ts / 10000000000000ULL);
}

// Extract day as YYYYMMDD from the timestamp.
inline uint32_t day_from_ts(uint64_t ts) {
  return static_cast<uint32_t>(ts / 1000000000ULL);
}

// Convert a day integer (YYYYMMDD) to a string "YYYY-MM-DD".
inline std::string day_to_string(uint32_t d) {
  uint32_t y  = d / 10000U;
  uint32_t m  = (d / 100U) % 100U;
  uint32_t dd = d % 100U;

  char buf[16];
  std::snprintf(buf, sizeof(buf), "%04u-%02u-%02u", y, m, dd);
  return std::string(buf);
}

}  // namespace nbbo
