#include "nbbo/histogram_model.hpp"

#include <algorithm>
#include <cmath>
#include <fstream>
#include <limits>
#include <stdexcept>

#include <nlohmann/json.hpp>

using nlohmann::json;

HistogramModel::HistogramModel(const std::string& json_path) {
  std::ifstream in(json_path);
  if (!in) {
    throw std::runtime_error("Failed to open histogram JSON: " + json_path);
  }

  json j;
  in >> j;

  // alpha is optional, default 1.0 if not present
  alpha = j.value("alpha", 1.0);

  if (!j.contains("cells") || !j["cells"].is_array()) {
    throw std::runtime_error("Histogram JSON missing 'cells' array");
  }

  const auto& jcells = j["cells"];
  if (static_cast<int>(jcells.size()) != N_CELLS) {
    throw std::runtime_error("Histogram JSON cells.size() != N_CELLS");
  }

  for (int k = 0; k < N_CELLS; ++k) {
    const auto& cj = jcells[k];
    CellStats cs;
    cs.n          = cj.value("n",          static_cast<std::uint64_t>(0));
    cs.n_up       = cj.value("n_up",       static_cast<std::uint64_t>(0));
    cs.n_down     = cj.value("n_down",     static_cast<std::uint64_t>(0));
    cs.sum_tau_ms = cj.value("sum_tau_ms", 0.0);
    cells[k] = cs;
  }
}

int HistogramModel::imb_bin(double I) const {
  // Clamp imbalance to [-1, 1] defensively
  if (I < -1.0) I = -1.0;
  if (I > 1.0) I = 1.0;

  if (I < -0.7)
    return 0;
  else if (I < -0.3)
    return 1;
  else if (I < -0.1)
    return 2;
  else if (I <= 0.1)
    return 3;
  else if (I <= 0.3)
    return 4;
  else
    return 5;
}

int HistogramModel::spr_bin(double spread) const {
  // spread in dollars; bin by ticks of 0.01
  constexpr double delta = 0.01;

  if (spread <= 0.0 || !std::isfinite(spread)) {
    // Treat nonpositive / NaN as 1-tick
    return 0;
  }

  int k = static_cast<int>(std::llround(spread / delta));
  if (k <= 1) return 0;  // 1 tick or less
  if (k == 2) return 1;  // 2 ticks
  return 2;              // 3+ ticks
}

int HistogramModel::age_bin(double age_diff_ms) const {
  // age_diff_ms = Age(bid) - Age(ask)
  if (age_diff_ms < -200.0)
    return 0;
  else if (age_diff_ms < -50.0)
    return 1;
  else if (age_diff_ms <= 50.0)
    return 2;
  else if (age_diff_ms <= 200.0)
    return 3;
  else
    return 4;
}

int HistogramModel::last_bin(double L) const {
  // L in {-1, 0, +1} ideally; threshold around 0
  if (L < -0.5) return 0;  // last move down
  if (L > 0.5) return 2;   // last move up
  return 1;                // no prior move / flat
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
