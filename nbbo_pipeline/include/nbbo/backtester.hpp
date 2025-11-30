// nbbo_pipeline/include/nbbo/backtester.hpp
#pragma once

#include <cstdint>
#include <string>
#include <vector>

#include "nbbo/event_types.hpp"
#include "nbbo/histogram_model.hpp"

namespace nbbo {

struct StrategyConfig {
  double fee_price = 0.03;
  double slip_price = 0.02;
  double min_abs_direction_score = 0.0;
  double min_expected_edge_bps = 0.0;
  double max_mean_wait_ms = 0.0;
};

StrategyConfig LoadStrategyConfig(const std::string& path);

struct TradeRecord {
  uint64_t ts_in = 0;
  uint64_t ts_out = 0;
  uint32_t day = 0;

  double mid_in = 0.0;
  double mid_out = 0.0;
  double spread_in = 0.0;

  double direction_score = 0.0;
  double expected_edge_ret = 0.0;
  double cost_ret = 0.0;
  double gross_ret = 0.0;
  double net_ret = 0.0;

  int side = 0;
};

struct DailyPnlRow {
  uint32_t day = 0;
  uint64_t num_trades = 0;
  double gross_ret_sum = 0.0;
  double net_ret_sum = 0.0;
  double gross_ret_mean = 0.0;
  double net_ret_mean = 0.0;
  double cumulative_net_ret = 0.0;
};

class PnLAggregator {
 public:
  PnLAggregator(std::string trades_out_dir, std::string daily_out_dir);
  void StartYear(uint32_t year);
  void OnTrade(const TradeRecord& trade);
  void FinalizeYear();

 private:
  void FlushCurrentDay();
  void WriteTradesCsv() const;
  void WriteDailyCsv() const;

  std::string trades_out_dir_;
  std::string daily_out_dir_;

  uint32_t year_ = 0;
  std::vector<TradeRecord> trades_;
  std::vector<DailyPnlRow> daily_rows_;

  uint32_t current_day_ = 0;
  uint64_t day_trade_count_ = 0;
  double day_gross_sum_ = 0.0;
  double day_net_sum_ = 0.0;

  double cumulative_net_ = 0.0;
};

class Backtester {
 public:
  Backtester(const HistogramModel& hist,
             const StrategyConfig& cfg,
             std::string trades_out_dir,
             std::string daily_out_dir);

  void RunForYear(uint32_t year, const std::string& events_path);

 private:
  void ProcessEvent(const nbbo::LabeledEvent& ev,
                    const nbbo::LabeledEvent* next_event);

  const HistogramModel& hist_;
  StrategyConfig cfg_;
  PnLAggregator pnl_;
};

}  // namespace nbbo
