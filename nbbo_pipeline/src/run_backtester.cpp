// nbbo_pipeline/src/run_backtester.cpp
//
// Small CLI wrapper around nbbo::Backtester.
//
// Responsibilities:
//   - Parse command-line args (events dir, histogram path, strategy config, year range)
//   - Construct HistogramModel + StrategyConfig
//   - Loop over years and call Backtester::RunForYear for each
//   - Write per-trade and per-day CSVs into data/research/trades and data/research/pnl

#include <cstdint>
#include <filesystem>
#include <iostream>
#include <stdexcept>
#include <string>

#include "nbbo/backtester.hpp"
#include "nbbo/histogram_model.hpp"

namespace {

// Simple path join for building "<events_dir>/SPY_<year>_events.parquet".
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
  // Expect exactly 5 arguments:
  //   1: events_dir
  //   2: histogram_json
  //   3: strategy_config_json
  //   4: start_year
  //   5: end_year
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

    // Load strategy parameters from JSON:
    //   fee_price, slip_price, min_abs_direction_score,
    //   min_expected_edge_bps, max_mean_wait_ms, etc.
    nbbo::StrategyConfig cfg = nbbo::LoadStrategyConfig(cfg_path);

    // Load histogram model from JSON:
    //   - direction_score(state)
    //   - mean_tau_ms(state)
    HistogramModel hist(hist_path);

    // Hard-coded output directories for:
    //   - per-trade CSVs (trades_out_dir)
    //   - per-day PnL CSVs (daily_out_dir)
    //
    // These are used inside nbbo::PnLAggregator.
    const std::string trades_out_dir = "data/research/trades";
    const std::string daily_out_dir  = "data/research/pnl";

    // Construct backtester with:
    //   - histogram model
    //   - strategy config
    //   - output directories
    nbbo::Backtester backtester(hist, cfg, trades_out_dir, daily_out_dir);

    // Main loop: run the backtest for each year in [start_year, end_year],
    // reading SPY_<year>_events.parquet from events_dir.
    for (int year = start_year; year <= end_year; ++year) {
      std::cout << "Running backtester for year " << year << "...\n";
      std::string fname = "SPY_" + std::to_string(year) + "_events.parquet";
      const std::string events_path = JoinPath(events_dir, fname);
      backtester.RunForYear(static_cast<uint32_t>(year), events_path);
    }

    std::cout << "Backtesting complete.\n";
    return 0;
  } catch (const std::exception& ex) {
    // Catch any exceptions (I/O, Arrow/parquet errors, config issues, etc.)
    // and print a clear message for debugging.
    std::cerr << "Error in run_backtester: " << ex.what() << "\n";
    return 1;
  }
}
