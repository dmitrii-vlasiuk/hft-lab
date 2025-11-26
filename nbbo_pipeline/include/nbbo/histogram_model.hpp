#pragma once
#include <array>
#include <cstdint>

struct CellStats {
  std::uint64_t n = 0;       // total count N_k
  std::uint64_t n_up = 0;    // count of Y_t = +1
  std::uint64_t n_down = 0;  // count of Y_t = -1
  double sum_tau_ms = 0.0;   // total waiting time (ms)
};

// State vector x_t = (I_t, s_t, age_diff_t, L_t)
struct TickState {
  double imbalance;    // I_t
  double spread;       // s_t
  double age_diff_ms;  // age_diff_ms = Age(bid) - Age(ask)
  double last_move;    // L_t in {-1,0,+1}
};

struct HistogramModel {
  static constexpr int N_IMB = 6;
  static constexpr int N_SPR = 3;
  static constexpr int N_AGE = 5;
  static constexpr int N_LAST = 3;
  static constexpr int N_CELLS = N_IMB * N_SPR * N_AGE * N_LAST;

  std::array<CellStats, N_CELLS> cells{};
  double alpha = 1.0;  // Laplace smoothing

  // binning (primitive interface)
  int imb_bin(double I) const;
  int spr_bin(double spread) const;
  int age_bin(double age_diff_ms) const;
  int last_bin(double L) const;
  int cell_index(double I, double s, double age_diff_ms, double L) const;

  // binning (state-based interface)
  int cell_index(const TickState& x) const;

  // derived quantities for a given cell index
  double p_up(int k) const;
  double p_down(int k) const;
  double direction_score(int k) const;  // D(k) = 2 p_up(k) - 1
  double mean_tau_ms(int k) const;      // E_hat[tau | k]

  // derived quantities from a state x_t
  double p_up(const TickState& x) const;
  double p_down(const TickState& x) const;
  double direction_score(const TickState& x) const;
  double mean_tau_ms(const TickState& x) const;
};
