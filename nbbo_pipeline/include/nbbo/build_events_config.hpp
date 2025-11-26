#pragma once
#include <string>

struct BuildEventsConfig {
  // Path to the cleaned per-ms nbbo input file
  std::string in_path;

  // Path to the per-event output file written by EventTableBuilder
  std::string out_path;

  // Acts as an outlier filter for anything outside of the threshold.
  // Tracks max absolute mid-price change between events
  // if |mid_next - mid| > threshold_next, the event is dropped.
  double threshold_next = 1.0;
};
