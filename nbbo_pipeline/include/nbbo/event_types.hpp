#pragma once
#include <cstdint>

namespace nbbo {

// One mid-change event used for generating labeled events.
// Created when logret != 0
struct LabeledEvent {
  uint64_t ts;         // Timestamp of the mid-change
  uint32_t day;        // YYYYMMDD for grouping by trading day
  double mid;          // Mid-price at event time
  double mid_next;     // Mid-price at the next mid-change event (same day)
  double spread;       // ask - bid at event time
  double imbalance;    // (bid_size - ask_size) / (bid_size + ask_size)
  double age_diff_ms;  // Age(bid) - Age(ask) in ms
  double last_move;    // Prev mid-move direction: {-1, 0, +1}
  double y;            // Sign(mid_next - mid): {-1, 0, +1}
  double tau_ms;       // Time until next mid-change (ms)
};

}  // namespace nbbo
