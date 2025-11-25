#pragma once
#include <cstdint>

namespace nbbo {

struct NbboTick {
  uint64_t ts;
  double mid;
  double logret;
  double bid_size;
  double ask_size;
  double spread;
  double bid;
  double ask;
};

struct LabeledEvent {
  uint64_t ts;
  uint32_t day;
  double mid;
  double mid_next;
  double spread;
  double imbalance;
  double age_diff_ms;
  double last_move;  // -1, 0, +1
  double y;          // label sign(delta-mid)
  double tau_ms;     // waiting time until next mid move
};

}  // namespace nbbo
