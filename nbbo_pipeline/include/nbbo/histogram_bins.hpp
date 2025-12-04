#pragma once

#include <array>
#include <nlohmann/json.hpp>
#include <string>

constexpr int HIST_N_IMB = 6;
constexpr int HIST_N_SPR = 3;
constexpr int HIST_N_AGE = 5;
constexpr int HIST_N_LAST = 3;

struct ImbBin {
  double lo;
  double hi;
  bool lo_inclusive;
  bool hi_inclusive;
  std::string interval;
};

struct SpreadBin {
  int ticks_min;
  int ticks_max;
  bool max_is_inf;
};

struct AgeBin {
  double lo;
  double hi;
  bool lo_is_inf;
  bool hi_is_inf;
  bool lo_inclusive;
  bool hi_inclusive;
};

struct LastMoveThresholds {
  double down_cut;
  double up_cut;
};

struct HistogramBinSpec {
  std::array<ImbBin, HIST_N_IMB> imb;
  std::array<SpreadBin, HIST_N_SPR> spr;
  std::array<AgeBin, HIST_N_AGE> age;
  LastMoveThresholds last;
};

// default bins
HistogramBinSpec make_default_histogram_bins();

// load/save from/to JSON object
HistogramBinSpec bins_from_json(const nlohmann::json& j);
nlohmann::json bins_to_json(const HistogramBinSpec& spec);
