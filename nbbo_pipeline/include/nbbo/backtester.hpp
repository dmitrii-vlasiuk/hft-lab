// nbbo_pipeline/include/nbbo/backtester.hpp
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "nbbo/event_types.hpp"
#include "nbbo/histogram_model.hpp"

namespace nbbo {

  enum class EdgeMode {
    Legacy        = 0,  // no costs, gate on expected_edge_ret > 0
    CostTradeAll  = 1,  // costs on, trade all
    CostWithGate  = 2   // costs on, absolute expected-edge gate
  };

  // High–level knobs for the strategy.
  // These are loaded from config/strategy_params.json by LoadStrategyConfig.
  struct StrategyConfig {
    // Per–leg fee in *price units* (e.g. $0.01 per share).
    // In the backtester we use 2 * fee_price to approximate a roundtrip.
    double fee_price = 0.03;

    // Extra “slippage cushion” in *price units*, charged once per roundtrip.
    double slip_price = 0.02;

    // Minimum |D(k)| (absolute histogram direction_score) required
    // to even consider a trade. If set to 0.0, this gate is disabled.
    double min_abs_direction_score = 0.0;

    // Expected-edge gate (in *basis points* of notional).
    // If == 0.0, the backtester will *not* apply an expected-edge filter
    // and will trade every event that passes other gates, simply deducting costs.
    //
    // If > 0.0, we require |EE| > (cost + min_expected_edge_bps) before trading.
    double min_expected_edge_bps = 0.0;

    // Optional filter on expected waiting time (in ms) from the histogram.
    // If > 0.0, we skip trades in states where mean_tau_ms(state) > max_mean_wait_ms.
    // If == 0.0, this filter is disabled.
    double max_mean_wait_ms = 0.0;

    // Edge-mode selector:
    EdgeMode edge_mode = EdgeMode::CostWithGate; // default to Mode B
  };

  // Load StrategyConfig from a flat JSON file (no nested objects).
  // This uses a very lightweight parser (ExtractDouble) rather than a full JSON lib.
  StrategyConfig LoadStrategyConfig(const std::string& path);

  // Per–trade record written out to SPY_YYYY_trades.csv.
  struct TradeRecord {
    // Event timestamps (nanoseconds since epoch, consistent with events parquet).
    uint64_t ts_in = 0;
    uint64_t ts_out = 0;

    // Trading day (YYYYMMDD encoded as uint32 in the input events).
    uint32_t day = 0;

    // Mid prices at entry/exit.
    double mid_in = 0.0;
    double mid_out = 0.0;

    // Spread at entry.
    double spread_in = 0.0;

    // Histogram signal D(k) at entry.
    double direction_score = 0.0;

    // Expected edge in return space (per $1 notional), based on histogram
    // and the 1-tick delta approximation.
    double expected_edge_ret = 0.0;

    // Total cost of a roundtrip in return space (spread + fee + slippage).
    double cost_ret = 0.0;

    // Realized one-step return in the direction of the trade.
    double gross_ret = 0.0;

    // Realized return after subtracting cost_ret.
    double net_ret = 0.0;

    // Trade direction: +1 for long, -1 for short.
    int side = 0;
  };

  // One row per trading day written to SPY_YYYY_daily.csv.
  struct DailyPnlRow {
    uint32_t day = 0;          // Trading day (same encoding as TradeRecord.day)
    uint64_t num_trades = 0;   // Number of trades on that day

    // Sum of gross and net returns over all trades that day.
    double gross_ret_sum = 0.0;
    double net_ret_sum = 0.0;

    // Mean gross/net return per trade for that day.
    double gross_ret_mean = 0.0;
    double net_ret_mean = 0.0;

    // Cumulative net return up through this day (running PnL).
    double cumulative_net_ret = 0.0;
  };

  // Aggregates per–trade PnL into daily rows and writes CSV outputs.
  class PnLAggregator {
  public:
    // trades_out_dir: directory where SPY_YYYY_trades.csv will be written.
    // daily_out_dir:  directory where SPY_YYYY_daily.csv will be written.
    PnLAggregator(std::string trades_out_dir, std::string daily_out_dir);

    // Initialize internal state for a new year and clear previous PnL state.
    void StartYear(uint32_t year);

    // Consume a single trade. This updates both the list of trades and
    // running daily/cumulative aggregates.
    void OnTrade(const TradeRecord& trade);

    // Flush the last day (if needed) and write both trades + daily CSVs to disk.
    void FinalizeYear();

  private:
    // Flush the currently open day into daily_rows_ (if it has any trades).
    void FlushCurrentDay();

    // Write SPY_<year>_trades.csv.
    void WriteTradesCsv() const;

    // Write SPY_<year>_daily.csv.
    void WriteDailyCsv() const;

    std::string trades_out_dir_;
    std::string daily_out_dir_;

    uint32_t year_ = 0;

    // All trades for the year (used when writing the trades CSV).
    std::vector<TradeRecord> trades_;

    // One row per trading day for the year.
    std::vector<DailyPnlRow> daily_rows_;

    // State for building up the current day's aggregates.
    uint32_t current_day_ = 0;
    uint64_t day_trade_count_ = 0;
    double day_gross_sum_ = 0.0;
    double day_net_sum_ = 0.0;

    // Running cumulative net return across days.
    double cumulative_net_ = 0.0;
  };

  // Main backtest engine for workflow C.
  //
  // Given:
  //   - a HistogramModel (trained on historical data)
  //   - a StrategyConfig (costs + thresholds)
  //   - output directories for CSVs
  //
  // It streams labeled events from SPY_YYYY_events.parquet,
  // decides which events to trade, and calls PnLAggregator on each trade.
  class Backtester {
  public:
    Backtester(const HistogramModel& hist,
              const StrategyConfig& cfg,
              std::string trades_out_dir,
              std::string daily_out_dir);

    // Run a backtest for a single calendar year.
    // events_path points to SPY_YYYY_events.parquet.
    void RunForYear(uint32_t year, const std::string& events_path);

  private:
    // Decide whether to trade on event `ev` given the immediately following
    // event `next_event` (if it is on the same day). If the decision is to
    // trade, this constructs a TradeRecord and passes it to pnl_.OnTrade().
    void ProcessEvent(const nbbo::LabeledEvent& ev,
                      const nbbo::LabeledEvent* next_event);

    const HistogramModel& hist_;  // read-only reference to prebuilt histogram
    StrategyConfig cfg_;          // copy of strategy parameters
    PnLAggregator pnl_;           // handles PnL aggregation and CSV output
  };

}  // namespace nbbo
