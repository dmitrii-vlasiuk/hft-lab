#include <cstdio>
#include <cstdlib>
#include <exception>
#include <string>

#include "nbbo/histogram_builder.hpp"

static void usage_and_exit(const char* argv0) {
  std::fprintf(stderr,
               R"(Usage:
  %s --events-root <dir> --symbol <SYM> --years <YYYY:YYYY> --out <histogram.json> [--alpha <float>]

Description:
  Reads per-event Parquet files produced by build_events for the given
  symbol and year range, aggregates them into a 4D histogram model, and
  writes the result as a JSON file usable by backtesting code.

Example:
  %s --events-root data/research/events \
     --symbol SPY \
     --years 2018:2022 \
     --out data/research/hist/SPY_histogram.json \
     --alpha 1.0
)",
               argv0, argv0);
  std::exit(2);
}

static HistogramConfig parse_args(int argc, char** argv) {
  HistogramConfig cfg;
  cfg.year_lo = 0;
  cfg.year_hi = 0;

  for (int i = 1; i < argc; ++i) {
    std::string a = argv[i];
    if (a == "--events-root" && i + 1 < argc) {
      cfg.events_root = argv[++i];
    } else if (a == "--symbol" && i + 1 < argc) {
      cfg.symbol = argv[++i];
    } else if (a == "--years" && i + 1 < argc) {
      std::string y = argv[++i];
      auto pos = y.find(':');
      if (pos == std::string::npos) usage_and_exit(argv[0]);
      cfg.year_lo = std::stoi(y.substr(0, pos));
      cfg.year_hi = std::stoi(y.substr(pos + 1));
    } else if (a == "--out" && i + 1 < argc) {
      cfg.out_path = argv[++i];
    } else if (a == "--alpha" && i + 1 < argc) {
      cfg.alpha = std::stod(argv[++i]);
    } else if (a == "--help" || a == "-h") {
      usage_and_exit(argv[0]);
    } else {
      std::fprintf(stderr, "Unknown or incomplete arg: %s\n", a.c_str());
      usage_and_exit(argv[0]);
    }
  }

  if (cfg.events_root.empty() || cfg.symbol.empty() || cfg.out_path.empty() ||
      cfg.year_lo == 0 || cfg.year_hi == 0) {
    usage_and_exit(argv[0]);
  }

  return cfg;
}

int main(int argc, char** argv) {
  HistogramConfig cfg = parse_args(argc, argv);

  try {
    HistogramBuilder builder(cfg);
    builder.run();
  } catch (const std::exception& e) {
    std::fprintf(stderr, "FATAL: %s\n", e.what());
    return 1;
  }
  return 0;
}
