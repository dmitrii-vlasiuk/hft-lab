// nbbo_pipeline/src/pnl_aggregator.cpp
#include "nbbo/backtester.hpp"

#include <cassert>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <ranges>
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

  CheckInvariants();
}

void PnLAggregator::OnTrade(const TradeRecord& trade) {
  CheckInvariants();

  if (trade.day == 0) {
    // Ignore trades that don't have a calendar day.
    return;
  }

  // When the calendar day changes, close out the previous day's row.
  if (current_day_ == 0) {
    // First trade of the year.
    current_day_ = trade.day;
  } else if (trade.day != current_day_) {
    FlushCurrentDay();
    current_day_ = trade.day;
  }

  trades_.push_back(trade);

  // Update daily aggregations.
  ++day_trade_count_;
  day_gross_sum_ += trade.gross_ret;
  day_net_sum_   += trade.net_ret;
  cumulative_net_ += trade.net_ret;

  CheckInvariants();
}

void PnLAggregator::FlushCurrentDay() {
  // If there's no open day or no trades, there is nothing to flush.
  if (current_day_ == 0 || day_trade_count_ == 0) {
#ifndef NDEBUG
    // Invariant: when we say "nothing to flush", per-day counters must be zero.
    assert(day_trade_count_ == 0);
    assert(day_gross_sum_ == 0.0);
    assert(day_net_sum_ == 0.0);
#endif
    return;
  }

  // Aggregate stats for this day and push into daily_rows_.
  DailyPnlRow row;
  row.day              = current_day_;
  row.num_trades       = day_trade_count_;
  row.gross_ret_sum    = day_gross_sum_;
  row.net_ret_sum      = day_net_sum_;
  row.gross_ret_mean   =
      day_gross_sum_ / static_cast<double>(day_trade_count_);
  row.net_ret_mean     =
      day_net_sum_ / static_cast<double>(day_trade_count_);
  row.cumulative_net_ret = cumulative_net_;

  daily_rows_.push_back(row);

  // Reset per-day counters for next day. current_day_ is left as-is;
  // it will be updated on the next trade or cleared by StartYear.
  day_trade_count_ = 0;
  day_gross_sum_   = 0.0;
  day_net_sum_     = 0.0;

  CheckInvariants();
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

  std::ranges::for_each(trades_, [&](const TradeRecord& t) {
    out << t.ts_in << ',' << t.ts_out << ',' << t.day << ','
        << t.mid_in << ',' << t.mid_out << ',' << t.spread_in << ','
        << t.direction_score << ',' << t.expected_edge_ret << ','
        << t.cost_ret << ',' << t.gross_ret << ',' << t.net_ret << ','
        << t.side << '\n';
  });
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

  std::ranges::for_each(daily_rows_, [&](const DailyPnlRow& row) {
    out << row.day << ',' << row.num_trades << ','
        << row.gross_ret_sum << ',' << row.net_ret_sum << ','
        << row.gross_ret_mean << ',' << row.net_ret_mean << ','
        << row.cumulative_net_ret << '\n';
  });
}

void PnLAggregator::FinalizeYear() {
  // Flush last day and then write CSVs for this year.
  FlushCurrentDay();
  CheckInvariants();

  if (year_ == 0) return;
  WriteTradesCsv();
  WriteDailyCsv();
}

void PnLAggregator::CheckInvariants() const {
#ifndef NDEBUG
  // Invariant 1: if current_day_ == 0 then we must not have an open day.
  if (current_day_ == 0) {
    assert(day_trade_count_ == 0);
    assert(day_gross_sum_ == 0.0);
    assert(day_net_sum_ == 0.0);
  }

  // Invariant 2: if we say there are no trades for the current day,
  // per-day sums should be zero.
  if (day_trade_count_ == 0) {
    assert(day_gross_sum_ == 0.0);
    assert(day_net_sum_ == 0.0);
  }

  // Invariant 3: daily_rows_ is strictly increasing in day and days are > 0.
  uint32_t prev_day = 0;
  for (const auto& row : daily_rows_) {
    assert(row.day > 0);
    assert(row.day > prev_day);
    prev_day = row.day;
  }

  // Invariant 4: if there is an open day, its day must be after any flushed day.
  if (!daily_rows_.empty() && current_day_ != 0) {
    assert(current_day_ > daily_rows_.back().day);
  }
#endif
}

}  // namespace nbbo
