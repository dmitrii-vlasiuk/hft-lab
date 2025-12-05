// nbbo_pipeline/src/run_backtester.cpp
//
// Small CLI wrapper around nbbo::Backtester.
//
// Responsibilities:
//  - Parse command-line args (events dir, histogram path, strategy config, year range)
//  - Construct HistogramModel + StrategyConfig
//  - Loop over years and call Backtester::RunForYear for each
//  - Write per-trade and per-day CSVs into data/research/trades and data/research/pnl
//  - Record per-step timings and dump a timing report to disk.

#include <chrono>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "nbbo/backtester.hpp"
#include "nbbo/histogram_model.hpp"
#include "nbbo/timing.hpp"

namespace {

// Simple path join for building "/SPY_<year>_events.parquet".
std::string JoinPath(const std::string& a, const std::string& b) {
  if (a.empty()) return b;
  std::filesystem::path p(a);
  p /= b;
  return p.string();
}

// Print CLI usage/help message.
void PrintUsage(const char* prog) {
  std::cerr << "Usage:\n"
            << "  " << prog
            << " <events_dir> <histogram_json> <strategy_config_json>"
            << " <start_year> <end_year>\n\n"
            << "Example:\n"
            << "  " << prog
            << " data/research/events"
            << " data/research/hist/SPY_histogram.json"
            << " config/strategy_params.json 2018 2023\n";
}

}  // namespace

int main(int argc, char** argv) {
  using Clock = std::chrono::steady_clock;
  const auto program_start = Clock::now();

  // Expect exactly 5 arguments:
  // 1: events_dir
  // 2: histogram_json
  // 3: strategy_config_json
  // 4: start_year
  // 5: end_year
  if (argc != 6) {
    PrintUsage(argv[0]);
    return 1;
  }

  try {
    const std::string events_dir = argv[1];
    const std::string hist_path  = argv[2];
    const std::string cfg_path   = argv[3];
    const int start_year         = std::stoi(argv[4]);
    const int end_year           = std::stoi(argv[5]);

    if (start_year > end_year) {
      throw std::runtime_error("start_year must be <= end_year");
    }

    // High-level timing for the main work.
    NBBO_SCOPE_TIMER("run_backtester_main");

    // Load strategy parameters from JSON.
    nbbo::StrategyConfig cfg;
    {
      NBBO_SCOPE_TIMER("load_strategy_config");
      cfg = nbbo::LoadStrategyConfig(cfg_path);
    }

    // Load histogram model from JSON.
    HistogramModel hist(hist_path);  // Small enough to not micro-split.

    // Hard-coded output directories for:
    //  - per-trade CSVs (trades_out_dir)
    //  - per-day PnL CSVs (daily_out_dir)
    const std::string trades_out_dir = "data/research/trades";
    const std::string daily_out_dir  = "data/research/pnl";

    // Construct backtester with:
    //  - histogram model
    //  - strategy config
    //  - output directories
    using Strategy = nbbo::HistogramEdgeStrategy;
    nbbo::Backtester<Strategy> backtester(hist, cfg, trades_out_dir, daily_out_dir);

    // Main loop: run the backtest for each year in [start_year, end_year],
    // reading SPY_<year>_events.parquet from events_dir.
    for (int year = start_year; year <= end_year; ++year) {
      std::cout << "Running backtester for year " << year << "...\n";

      std::string fname = "SPY_" + std::to_string(year) + "_events.parquet";
      const std::string events_path = JoinPath(events_dir, fname);

      // Per-year timing.
      {
        NBBO_SCOPE_TIMER("RunForYear_" + std::to_string(year));
        backtester.RunForYear(static_cast<std::uint32_t>(year), events_path);
      }
    }

    std::cout << "Backtesting complete.\n";

    // Record total wall-clock time.
    const auto program_end = Clock::now();
    nbbo::TimingRegistry::Instance().Add("program_wall_clock",
                                         program_end - program_start);

    // Build argv vector for the report.
    std::vector<std::string> args;
    args.reserve(static_cast<std::size_t>(argc > 1 ? argc - 1 : 0));
    for (int i = 1; i < argc; ++i) {
      args.emplace_back(argv[i]);
    }

    // Write timing report (append to shared log).
    const std::string timing_path = "data/research/profile/timing_log.txt";
    nbbo::WriteTimingReport(timing_path, argv[0], args);

    return 0;
  } catch (const std::exception& ex) {
    std::cerr << "Error in run_backtester: " << ex.what() << "\n";
    return 1;
  }
}
