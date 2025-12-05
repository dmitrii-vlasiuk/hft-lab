// nbbo_pipeline/src/pnl_aggregator.cpp
#include "nbbo/backtester.hpp"

#include <filesystem>
#include <fstream>
#include <iomanip>
#include <sstream>
#include <stdexcept>
#include <string>

namespace {

// Simple path join helper used when writing CSVs.
inline std::string JoinPath(const std::string& a, const std::string& b) {
  if (a.empty()) return b;
  std::filesystem::path p(a);
  p /= b;
  return p.string();
}

}  // namespace

namespace nbbo {

PnLAggregator::PnLAggregator(std::string trades_out_dir,
                             std::string daily_out_dir)
    : trades_out_dir_(std::move(trades_out_dir)),
      daily_out_dir_(std::move(daily_out_dir)) {}

void PnLAggregator::StartYear(uint32_t year) {
  year_ = year;
  trades_.clear();
  daily_rows_.clear();
  current_day_ = 0;
  day_trade_count_ = 0;
  day_gross_sum_ = 0.0;
  day_net_sum_ = 0.0;
  cumulative_net_ = 0.0;
}

void PnLAggregator::OnTrade(const TradeRecord& trade) {
  if (trade.day == 0) return;

  // When the calendar day changes, close out the previous day's row.
  if (current_day_ == 0) {
    current_day_ = trade.day;
  } else if (trade.day != current_day_) {
    FlushCurrentDay();
    current_day_ = trade.day;
  }

  trades_.push_back(trade);

  // Update daily aggregations.
  day_trade_count_++;
  day_gross_sum_ += trade.gross_ret;
  day_net_sum_ += trade.net_ret;
  cumulative_net_ += trade.net_ret;
}

void PnLAggregator::FlushCurrentDay() {
  if (current_day_ == 0 || day_trade_count_ == 0) {
    // Nothing to flush (no trades for this day)
    return;
  }

  // Aggregate stats for this day and push into daily_rows_.
  DailyPnlRow row;
  row.day = current_day_;
  row.num_trades = day_trade_count_;
  row.gross_ret_sum = day_gross_sum_;
  row.net_ret_sum = day_net_sum_;
  row.gross_ret_mean =
      day_gross_sum_ / static_cast<double>(day_trade_count_);
  row.net_ret_mean =
      day_net_sum_ / static_cast<double>(day_trade_count_);
  row.cumulative_net_ret = cumulative_net_;

  daily_rows_.push_back(row);

  // Reset per-day counters for next day.
  day_trade_count_ = 0;
  day_gross_sum_ = 0.0;
  day_net_sum_ = 0.0;
}

void PnLAggregator::WriteTradesCsv() const {
  std::filesystem::create_directories(trades_out_dir_);

  // File name pattern: SPY_<year>_trades.csv
  std::ostringstream fname;
  fname << "SPY_" << year_ << "_trades.csv";
  const std::string path = JoinPath(trades_out_dir_, fname.str());

  std::ofstream out(path);
  if (!out) {
    throw std::runtime_error("Failed to open trades output: " + path);
  }

  // One row per trade; columns are all risk/PnL-relevant fields
  // in return space (except mid prices, spread, etc.).
  out << "ts_in,ts_out,day,mid_in,mid_out,spread_in,"
         "direction_score,expected_edge_ret,cost_ret,"
         "gross_ret,net_ret,side\n";

  out << std::setprecision(10);

  for (const auto& t : trades_) {
    out << t.ts_in << ',' << t.ts_out << ',' << t.day << ','
        << t.mid_in << ',' << t.mid_out << ',' << t.spread_in << ','
        << t.direction_score << ',' << t.expected_edge_ret << ','
        << t.cost_ret << ',' << t.gross_ret << ',' << t.net_ret << ','
        << t.side << '\n';
  }
}

void PnLAggregator::WriteDailyCsv() const {
  std::filesystem::create_directories(daily_out_dir_);

  // File name pattern: SPY_<year>_daily.csv
  std::ostringstream fname;
  fname << "SPY_" << year_ << "_daily.csv";
  const std::string path = JoinPath(daily_out_dir_, fname.str());

  std::ofstream out(path);
  if (!out) {
    throw std::runtime_error("Failed to open daily PnL output: " + path);
  }

  // One row per calendar day; includes per-day sums, means,
  // and a running cumulative net return.
  out << "day,num_trades,gross_ret_sum,net_ret_sum,"
         "gross_ret_mean,net_ret_mean,cumulative_net_ret\n";

  out << std::setprecision(10);

  for (const auto& row : daily_rows_) {
    out << row.day << ',' << row.num_trades << ','
        << row.gross_ret_sum << ',' << row.net_ret_sum << ','
        << row.gross_ret_mean << ',' << row.net_ret_mean << ','
        << row.cumulative_net_ret << '\n';
  }
}

void PnLAggregator::FinalizeYear() {
  // Flush last day and then write CSVs for this year.
  FlushCurrentDay();
  if (year_ == 0) return;
  WriteTradesCsv();
  WriteDailyCsv();
}

}  // namespace nbbo
