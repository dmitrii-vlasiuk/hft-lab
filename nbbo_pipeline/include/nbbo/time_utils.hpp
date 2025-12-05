#pragma once

#include <cstdint>
#include <string>
#include <cstdio>   // std::snprintf
#include <chrono>

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
//   - Incrementing a timestamp by 1 ms (intraday, no calendar roll)
//   - Conversion between day integers (YYYYMMDD) and strings ("YYYY-MM-DD")
//   - Modern C++20 <chrono>-based wrappers for working with timestamps.

// --------------------- Low-level integer helpers ---------------------

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

  // Extract calendar date "as stored".
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
  // YYYYMMDDHHMMSSmmm -> drop MMDDHHMMSSmmm (13 digits)
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

// --------------------- Modern chrono wrappers ---------------------

using TimePointMs = std::chrono::sys_time<std::chrono::milliseconds>;

// Simple POD struct for broken-down timestamp fields.
struct TimestampParts {
  int year;          // four-digit year
  unsigned month;    // 1–12
  unsigned day;      // 1–31
  int hour;          // 0–23
  int minute;        // 0–59
  int second;        // 0–59
  int millisecond;   // 0–999
};

// Decode the custom integer-encoded timestamp into fields.
inline TimestampParts decode_timestamp(uint64_t ts) {
  TimestampParts p{};

  uint32_t day_int = day_from_ts(ts);
  p.year           = static_cast<int>(day_int / 10000U);
  p.month          = static_cast<unsigned>((day_int / 100U) % 100U);
  p.day            = static_cast<unsigned>(day_int % 100U);

  p.hour        = hh(ts);
  p.minute      = mm(ts);
  p.second      = ss(ts);
  p.millisecond = mmm(ts);

  return p;
}

// Convert a YYYYMMDD day integer to std::chrono::year_month_day.
inline std::chrono::year_month_day day_to_ymd(uint32_t d) {
  using namespace std::chrono;
  int y        = static_cast<int>(d / 10000U);
  unsigned m   = static_cast<unsigned>((d / 100U) % 100U);
  unsigned dd  = static_cast<unsigned>(d % 100U);
  return year_month_day{year{y}, month{m}, day{dd}};
}

// Convert std::chrono::year_month_day back to an integer YYYYMMDD.
inline uint32_t ymd_to_day(const std::chrono::year_month_day& ymd) {
  int y        = static_cast<int>(ymd.year());
  unsigned m   = static_cast<unsigned>(ymd.month());
  unsigned dd  = static_cast<unsigned>(ymd.day());
  return static_cast<uint32_t>(y * 10000 + m * 100 + dd);
}

// Convert an integer-encoded timestamp to a chrono time_point in UTC.
inline TimePointMs ts_to_time_point(uint64_t ts) {
  using namespace std::chrono;

  TimestampParts p = decode_timestamp(ts);
  year_month_day ymd{year{p.year}, month{p.month}, day{p.day}};

  // sys_days is days since Unix epoch.
  sys_days d{ymd};

  return d + hours{p.hour} +
         minutes{p.minute} +
         seconds{p.second} +
         milliseconds{p.millisecond};
}

// Convert a chrono time_point in UTC back to the integer timestamp encoding.
inline uint64_t time_point_to_ts(TimePointMs tp) {
  using namespace std::chrono;

  using days = std::chrono::days;
  auto d    = floor<days>(tp);
  auto tod  = hh_mm_ss<milliseconds>(tp - d);

  year_month_day ymd{d};

  int y        = static_cast<int>(ymd.year());
  unsigned m   = static_cast<unsigned>(ymd.month());
  unsigned dd  = static_cast<unsigned>(ymd.day());

  int H        = static_cast<int>(tod.hours().count());
  int M        = static_cast<int>(tod.minutes().count());
  int S        = static_cast<int>(tod.seconds().count());
  int MS       = static_cast<int>(tod.subseconds().count());

  uint32_t day_int = static_cast<uint32_t>(y * 10000 + m * 100 + dd);

  return static_cast<uint64_t>(day_int) * 1000000000ULL +
         static_cast<uint64_t>(H)       * 10000000ULL   +
         static_cast<uint64_t>(M)       * 100000ULL     +
         static_cast<uint64_t>(S)       * 1000ULL       +
         static_cast<uint64_t>(MS);
}

// Chrono version of ms_since_midnight: returns a duration.
inline std::chrono::milliseconds ms_since_midnight_chrono(uint64_t ts) {
  return std::chrono::milliseconds(ms_since_midnight(ts));
}

// Add an arbitrary millisecond duration using chrono, then convert back
// to the integer NBBO timestamp encoding.
inline uint64_t add_ms_chrono(uint64_t ts,
                              std::chrono::milliseconds delta) {
  TimePointMs tp = ts_to_time_point(ts);
  tp += delta;
  return time_point_to_ts(tp);
}

}  // namespace nbbo
