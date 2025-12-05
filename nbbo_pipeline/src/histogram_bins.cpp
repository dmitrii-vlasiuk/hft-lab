// histogram_bins.cpp
//
// Defines the default 4D binning scheme for the histogram model
// (imbalance, spread, age_diff_ms, last_move) and provides JSON load/save helpers for those bin specs.

#include "nbbo/histogram_bins.hpp"

#include <climits>
#include <stdexcept>

using nlohmann::json;

HistogramBinSpec make_default_histogram_bins() {
  HistogramBinSpec spec;

  // Imbalance bins
  auto set_imb = [&](int idx, double lo, double hi, bool lo_inc, bool hi_inc,
                     const char* interval) {
    auto& b = spec.imb[idx];
    b.lo = lo;
    b.hi = hi;
    b.lo_inclusive = lo_inc;
    b.hi_inclusive = hi_inc;
    b.interval = interval;
  };

  set_imb(0, -1.0, -0.7, true, false, "[-1.0, -0.7)");
  set_imb(1, -0.7, -0.3, true, false, "[-0.7, -0.3)");
  set_imb(2, -0.3, -0.1, true, false, "[-0.3, -0.1)");
  set_imb(3, -0.1, 0.1, true, true, "[-0.1, 0.1]");
  set_imb(4, 0.1, 0.3, false, true, "(0.1, 0.3]");
  set_imb(5, 0.3, 1.0, false, true, "(0.3, 1.0]");

  // Spread bins
  spec.spr[0] = SpreadBin{0, 1, false};
  spec.spr[1] = SpreadBin{2, 2, false};
  spec.spr[2] = SpreadBin{3, 0, true};

  // Age-diff bins
  auto set_age = [&](int idx, double lo, double hi, bool lo_inf, bool hi_inf,
                     bool lo_inc, bool hi_inc) {
    auto& b = spec.age[idx];
    b.lo = lo;
    b.hi = hi;
    b.lo_is_inf = lo_inf;
    b.hi_is_inf = hi_inf;
    b.lo_inclusive = lo_inc;
    b.hi_inclusive = hi_inc;
  };
  set_age(0, 0.0, -200.0, true, false, false, false);
  set_age(1, -200.0, -50.0, false, false, true, false);
  set_age(2, -50.0, 50.0, false, false, true, true);
  set_age(3, 50.0, 200.0, false, false, false, true);
  set_age(4, 200.0, 0.0, false, true, false, false);

  // Last-move thresholds
  spec.last.down_cut = -0.5;
  spec.last.up_cut = 0.5;

  return spec;
}

HistogramBinSpec bins_from_json(const json& j_root) {
  HistogramBinSpec spec = make_default_histogram_bins();

  // Imbalance
  if (j_root.contains("imbalance_bins")) {
    const auto& arr = j_root.at("imbalance_bins");
    if (!arr.is_array() || arr.size() != HIST_N_IMB) {
      throw std::runtime_error("imbalance_bins has wrong size");
    }
    for (const auto& jb : arr) {
      int idx = jb.at("idx").get<int>();
      if (idx < 0 || idx >= HIST_N_IMB) {
        throw std::runtime_error("imbalance_bins idx out of range");
      }
      auto& b = spec.imb[idx];
      b.lo = jb.at("lo").get<double>();
      b.hi = jb.at("hi").get<double>();
      b.interval = jb.value("interval", std::string{});

      if (!b.interval.empty()) {
        b.lo_inclusive = (!b.interval.empty() && b.interval.front() == '[');
        char last = b.interval.back();
        b.hi_inclusive = (last == ']');
      } else {
        b.lo_inclusive = true;
        b.hi_inclusive = true;
      }
    }
  }

  // Spread
  if (j_root.contains("spread_bins")) {
    const auto& arr = j_root.at("spread_bins");
    if (!arr.is_array() || arr.size() != HIST_N_SPR) {
      throw std::runtime_error("spread_bins has wrong size");
    }
    for (const auto& jb : arr) {
      int idx = jb.at("idx").get<int>();
      if (idx < 0 || idx >= HIST_N_SPR) {
        throw std::runtime_error("spread_bins idx out of range");
      }
      auto& b = spec.spr[idx];
      b.ticks_min = jb.at("ticks_min").get<int>();
      if (jb.at("ticks_max").is_null()) {
        b.max_is_inf = true;
        b.ticks_max = 0;
      } else {
        b.max_is_inf = false;
        b.ticks_max = jb.at("ticks_max").get<int>();
      }
    }
  }

  // Age
  if (j_root.contains("age_diff_ms_bins")) {
    const auto& arr = j_root.at("age_diff_ms_bins");
    if (!arr.is_array() || arr.size() != HIST_N_AGE) {
      throw std::runtime_error("age_diff_ms_bins has wrong size");
    }
    for (const auto& jb : arr) {
      int idx = jb.at("idx").get<int>();
      if (idx < 0 || idx >= HIST_N_AGE) {
        throw std::runtime_error("age_diff_ms_bins idx out of range");
      }
      auto& b = spec.age[idx];

      if (jb.at("lo").is_null()) {
        b.lo_is_inf = true;
        b.lo = 0.0;
      } else {
        b.lo_is_inf = false;
        b.lo = jb.at("lo").get<double>();
      }

      if (jb.at("hi").is_null()) {
        b.hi_is_inf = true;
        b.hi = 0.0;
      } else {
        b.hi_is_inf = false;
        b.hi = jb.at("hi").get<double>();
      }
    }
  }

  return spec;
}

nlohmann::json bins_to_json(const HistogramBinSpec& spec) {
  json j;

  // Imbalance
  json imb = json::array();
  for (int b = 0; b < HIST_N_IMB; ++b) {
    const auto& bin = spec.imb[b];
    imb.push_back(json{
        {"idx", b},
        {"lo", bin.lo},
        {"hi", bin.hi},
        {"interval", bin.interval},
    });
  }
  j["imbalance_bins"] = std::move(imb);

  // Spread
  json spr = json::array();
  for (int b = 0; b < HIST_N_SPR; ++b) {
    const auto& bin = spec.spr[b];
    json obj;
    obj["idx"] = b;
    obj["ticks_min"] = bin.ticks_min;
    if (bin.max_is_inf) {
      obj["ticks_max"] = nullptr;
    } else {
      obj["ticks_max"] = bin.ticks_max;
    }
    spr.push_back(std::move(obj));
  }
  j["spread_bins"] = std::move(spr);

  // Age
  json age = json::array();
  for (int b = 0; b < HIST_N_AGE; ++b) {
    const auto& bin = spec.age[b];
    json obj;
    obj["idx"] = b;
    obj["lo"] = bin.lo_is_inf ? json(nullptr) : json(bin.lo);
    obj["hi"] = bin.hi_is_inf ? json(nullptr) : json(bin.hi);
    age.push_back(std::move(obj));
  }
  j["age_diff_ms_bins"] = std::move(age);

  // keep existing "last_move_bins" shape with L = -1, 0, +1
  j["last_move_bins"] = json::array({
      json{{"idx", 0}, {"L", -1}},
      json{{"idx", 1}, {"L", 0}},
      json{{"idx", 2}, {"L", 1}},
  });

  return j;
}
