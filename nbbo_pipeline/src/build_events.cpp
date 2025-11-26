#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>

#include "nbbo/build_events_config.hpp"
#include "nbbo/event_table_builder.hpp"

static void usage_and_exit(const char* argv0) {
  std::fprintf(stderr,
               R"(Usage:
  %s --in <input_clean.parquet> --out <events.parquet>
       [--threshold-next <dollars>]

Description:
  Reads a cleaned per-ms NBBO Parquet file (event grid) and constructs
  per-mid-change events on each day. For each mid-change event `t`
  (nonzero log-return) it:
    - Computes volume imbalance I_t  = (bid_size - ask_size) / (bid_size + ask_size)
    - Uses the spread s_t = ask - bid
    - Tracks ages of the current best bid/ask quotes in milliseconds
      since those prices first appeared, and forms delta_a_t = a^bid_t - a^ask_t
    - Maintains the last-move sign L_t: the sign of the previous mid move
      within the same day (0 for the first move of the day)

  For labeling, it finds the next mid-change on the same day, sets:
    - mid_next_t = mid_next
    - Y_t = sign(mid_next_t - mid_t)
    - tau_t = time difference (ms) to the next mid-change

  It drops:
    - The last mid-change of each day (no next move on same day)
    - Any event where |mid_next_t - mid_t| > threshold-next

Example:
  %s --in data/out/event_clean/SPY_2020.parquet \
     --out data/research/events/SPY_2020_events.parquet \
     --threshold-next 1.0
)",
               argv0, argv0);
  std::exit(2);
}

static BuildEventsConfig parse_args(int argc, char** argv) {
  // Minimal command-line parser that populates BuildEventsConfig.

  BuildEventsConfig cfg;

  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--in" && i + 1 < argc) {
      cfg.in_path = argv[++i];
    } else if (a == "--out" && i + 1 < argc) {
      cfg.out_path = argv[++i];
    } else if (a == "--threshold-next" && i + 1 < argc) {
      cfg.threshold_next = std::stod(argv[++i]);
    } else if (a == "--help" || a == "-h") {
      usage_and_exit(argv[0]);
    } else {
      std::fprintf(stderr, "Unknown or incomplete arg: %s\n", a.c_str());
      usage_and_exit(argv[0]);
    }
  }

  if (cfg.in_path.empty() || cfg.out_path.empty()) {
    usage_and_exit(argv[0]);
  }

  return cfg;
}

int main(int argc, char** argv) {
  // Parse args and run the event builder pipeline
  BuildEventsConfig cfg = parse_args(argc, argv);

  try {
    // EventTableBuilder performs the following:
    // - Reads the cleaned nbbo files
    // - Detects mid-change events
    // - Generates and labels features (events)
    // - Writes final output to parquet file

    EventTableBuilder builder(cfg);
    builder.run();

  } catch (const std::exception& e) {
    std::fprintf(stderr, "FATAL: %s\n", e.what());
    return 1;
  }
  return 0;
}
