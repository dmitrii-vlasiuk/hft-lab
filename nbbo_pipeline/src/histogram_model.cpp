// histogram_model.cpp
//
// Implements HistogramModel, a binned, non-parametric model of short-horizon
// price dynamics conditioned on LOB state.

#include "nbbo/histogram_model.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <limits>
#include <nlohmann/json.hpp>
#include <ranges>
#include <stdexcept>

#include "nbbo/histogram_bins.hpp"

using nlohmann::json;
namespace rng = std::ranges;
namespace vw  = std::views;

HistogramModel::HistogramModel() : bins(make_default_histogram_bins()) {}

HistogramModel::HistogramModel(const std::string& json_path)
    : bins(make_default_histogram_bins()) {
  std::ifstream in(json_path);
  if (!in) {
    throw std::runtime_error("Failed to open histogram JSON: " + json_path);
  }

  json j;
  in >> j;

  // alpha is optional, default 1.0 if not present
  alpha = j.value("alpha", 1.0);

  if (j.contains("imbalance_bins") || j.contains("spread_bins") ||
      j.contains("age_diff_ms_bins") || j.contains("last_move_bins")) {
    bins = bins_from_json(j);
  }

  if (!j.contains("cells") || !j["cells"].is_array()) {
    throw std::runtime_error("Histogram JSON missing 'cells' array");
  }

  const auto& jcells = j["cells"];
  if (static_cast<int>(jcells.size()) != N_CELLS) {
    throw std::runtime_error("Histogram JSON cells.size() != N_CELLS");
  }

  rng::for_each(vw::iota(0, N_CELLS), [&](int k) {
    const auto& cj = jcells[k];
    CellStats cs;
    cs.n          = cj.value("n",      static_cast<std::uint64_t>(0));
    cs.n_up       = cj.value("n_up",   static_cast<std::uint64_t>(0));
    cs.n_down     = cj.value("n_down", static_cast<std::uint64_t>(0));
    cs.sum_tau_ms = cj.value("sum_tau_ms", 0.0);
    cells[k]      = cs;
  });
}

int HistogramModel::imb_bin(double I) const {
  // Clamp imbalance to [-1, 1]
  if (I < -1.0) I = -1.0;
  if (I > 1.0) I = 1.0;

  auto idxs = vw::iota(0, N_IMB);

  auto it = rng::find_if(idxs, [&](int b) {
    const auto& bin = bins.imb[b];
    bool ok_lo = bin.lo_inclusive ? I >= bin.lo : I > bin.lo;
    bool ok_hi = bin.hi_inclusive ? I <= bin.hi : I < bin.hi;
    return ok_lo && ok_hi;
  });

  if (it != rng::end(idxs)) {
    return *it;
  }
  return N_IMB - 1;
}

int HistogramModel::spr_bin(double spread) const {
  // spread in dollars; bin by ticks of 0.01
  constexpr double delta = 0.01;

  if (spread <= 0.0 || !std::isfinite(spread)) {
    // Treat nonpositive / NaN as 1-tick
    return 0;
  }

  int k = static_cast<int>(std::llround(spread / delta));

  auto idxs = vw::iota(0, N_SPR);

  auto it = rng::find_if(idxs, [&](int b) {
    const auto& bin = bins.spr[b];
    if (k < bin.ticks_min) return false;
    if (!bin.max_is_inf && k > bin.ticks_max) return false;
    return true;
  });

  if (it != rng::end(idxs)) {
    return *it;
  }
  return N_SPR - 1;
}

int HistogramModel::age_bin(double age_diff_ms) const {
  // age_diff_ms = Age(bid) - Age(ask)
  auto idxs = vw::iota(0, N_AGE);

  auto it = rng::find_if(idxs, [&](int b) {
    const auto& bin = bins.age[b];
    bool ok_lo = bin.lo_is_inf ? true
                               : (bin.lo_inclusive ? age_diff_ms >= bin.lo
                                                   : age_diff_ms > bin.lo);
    bool ok_hi = bin.hi_is_inf ? true
                               : (bin.hi_inclusive ? age_diff_ms <= bin.hi
                                                   : age_diff_ms < bin.hi);
    return ok_lo && ok_hi;
  });

  if (it != rng::end(idxs)) {
    return *it;
  }
  return N_AGE - 1;
}

int HistogramModel::last_bin(double L) const {
  if (L < bins.last.down_cut) return 0;
  if (L > bins.last.up_cut) return 2;
  return 1;
}

int HistogramModel::cell_index(double I,
                               double s,
                               double age_diff_ms,
                               double L) const {
  int b_imb = imb_bin(I);
  int b_spr = spr_bin(s);
  int b_age = age_bin(age_diff_ms);
  int b_last = last_bin(L);
  return (((b_imb * N_SPR + b_spr) * N_AGE + b_age) * N_LAST + b_last);
}

int HistogramModel::cell_index(const TickState& x) const {
  return cell_index(x.imbalance, x.spread, x.age_diff_ms, x.last_move);
}

double HistogramModel::p_up(int k) const {
  const CellStats& c = cells[k];
  const double n_up = static_cast<double>(c.n_up);
  const double n_down = static_cast<double>(c.n_down);
  const double n_tot = n_up + n_down;

  if (n_tot <= 0.0 || !std::isfinite(n_tot)) {
    // Empty cell: symmetric prior
    return 0.5;
  }
  // Laplace smoothing
  return (n_up + alpha) / (n_tot + 2.0 * alpha);
}

double HistogramModel::p_down(int k) const { return 1.0 - p_up(k); }

double HistogramModel::direction_score(int k) const {
  // D(k)
  return 2.0 * p_up(k) - 1.0;
}

double HistogramModel::mean_tau_ms(int k) const {
  const CellStats& c = cells[k];
  if (c.n == 0) {
    return std::numeric_limits<double>::quiet_NaN();
  }
  return c.sum_tau_ms / static_cast<double>(c.n);
}

// State-based overloads
double HistogramModel::p_up(const TickState& x) const {
  return p_up(cell_index(x));
}

double HistogramModel::p_down(const TickState& x) const {
  return p_down(cell_index(x));
}

double HistogramModel::direction_score(const TickState& x) const {
  return direction_score(cell_index(x));
}

double HistogramModel::mean_tau_ms(const TickState& x) const {
  return mean_tau_ms(cell_index(x));
}
