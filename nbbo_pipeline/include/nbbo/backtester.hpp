// nbbo_pipeline/include/nbbo/backtester.hpp
#pragma once

#include <cstdint>
#include <string>
#include <vector>
#include <memory>
#include <optional>
#include <concepts>

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
  uint64_t ts_in  = 0;
  uint64_t ts_out = 0;

  // Trading day (YYYYMMDD encoded as uint32 in the input events).
  uint32_t day = 0;

  // Mid prices at entry/exit.
  double mid_in  = 0.0;
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

// --- Strategy-like concept ------------------------------------------------

// Any type S that provides:
//   std::optional<TradeRecord> OnEvent(const LabeledEvent&, const LabeledEvent*)
// qualifies as a strategy.
template <class S>
concept StrategyLike =
  requires(S s,
           const LabeledEvent& ev,
           const LabeledEvent* next) {
    { s.OnEvent(ev, next) } -> std::same_as<std::optional<TradeRecord>>;
  };

// Strategy interface: something that can decide whether to trade on (ev, next_event).
// (Still useful if you want dynamic polymorphism elsewhere.)
class Strategy {
public:
  virtual ~Strategy() = default;

  // Return a TradeRecord if we want to trade, or std::nullopt if we skip.
  virtual std::optional<TradeRecord>
  OnEvent(const LabeledEvent& ev,
          const LabeledEvent* next_event) = 0;
};

// Concrete implementation using the histogram model + StrategyConfig.
class HistogramEdgeStrategy final : public Strategy {
public:
  HistogramEdgeStrategy(const HistogramModel& hist,
                        StrategyConfig cfg);

  std::optional<TradeRecord>
  OnEvent(const LabeledEvent& ev,
          const LabeledEvent* next_event) override;

private:
  const HistogramModel& hist_;  // read-only reference to prebuilt histogram
  StrategyConfig        cfg_;   // local copy of strategy parameters
};

// Ensure at compile time that HistogramEdgeStrategy satisfies the concept.
static_assert(StrategyLike<HistogramEdgeStrategy>);

// One row per trading day written to SPY_YYYY_daily.csv.
struct DailyPnlRow {
  uint32_t day        = 0;   // Trading day (same encoding as TradeRecord.day)
  uint64_t num_trades = 0;   // Number of trades on that day

  // Sum of gross and net returns over all trades that day.
  double gross_ret_sum = 0.0;
  double net_ret_sum   = 0.0;

  // Mean gross/net return per trade for that day.
  double gross_ret_mean = 0.0;
  double net_ret_mean   = 0.0;

  // Cumulative net return up through this day (running PnL).
  double cumulative_net_ret = 0.0;
};

// Aggregates per–trade PnL into daily rows and writes CSV outputs.
class PnLAggregator {
public:
  PnLAggregator(std::string trades_out_dir,
                std::string daily_out_dir);

  void StartYear(uint32_t year);
  void OnTrade(const TradeRecord& trade);
  void FinalizeYear();

private:
  void FlushCurrentDay();
  void WriteTradesCsv() const;
  void WriteDailyCsv() const;
  void CheckInvariants() const;

  uint32_t year_ = 0;

  std::vector<TradeRecord> trades_;
  std::vector<DailyPnlRow> daily_rows_;

  uint32_t current_day_     = 0;
  int      day_trade_count_ = 0;
  double   day_gross_sum_   = 0.0;
  double   day_net_sum_     = 0.0;
  double   cumulative_net_  = 0.0;

  std::string trades_out_dir_;
  std::string daily_out_dir_;
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
template <StrategyLike S>
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
  void ProcessEvent(const nbbo::LabeledEvent& ev,
                    const nbbo::LabeledEvent* next_event);

  S             strategy_;  // concrete strategy, stored by value
  PnLAggregator pnl_;
};

}  // namespace nbbo
