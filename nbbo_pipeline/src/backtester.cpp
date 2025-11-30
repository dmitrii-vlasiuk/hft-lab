// nbbo_pipeline/src/backtester.cpp
#include "nbbo/backtester.hpp"

#include <arrow/api.h>
#include <parquet/arrow/reader.h>

#include <algorithm>
#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <stdexcept>

#include "nbbo/arrow_utils.hpp"

namespace {

double ExtractDouble(const std::string& json,
                     const std::string& key,
                     double default_value) {
  const std::string quoted_key = "\"" + key + "\"";
  auto pos = json.find(quoted_key);
  if (pos == std::string::npos) {
    return default_value;
  }
  pos = json.find(':', pos);
  if (pos == std::string::npos) {
    return default_value;
  }
  ++pos;
  while (pos < json.size() && std::isspace(static_cast<unsigned char>(json[pos]))) {
    ++pos;
  }

  std::string number_str;
  while (pos < json.size()) {
    char c = json[pos];
    if ((c >= '0' && c <= '9') || c == '-' || c == '+' || c == '.' || c == 'e' ||
        c == 'E') {
      number_str.push_back(c);
      ++pos;
    } else {
      break;
    }
  }

  if (number_str.empty()) {
    return default_value;
  }

  try {
    return std::stod(number_str);
  } catch (...) {
    return default_value;
  }
}

inline std::string JoinPath(const std::string& a, const std::string& b) {
  if (a.empty()) return b;
  std::filesystem::path p(a);
  p /= b;
  return p.string();
}

}  // namespace

namespace nbbo {

// ------------------------- StrategyConfig -------------------------

StrategyConfig LoadStrategyConfig(const std::string& path) {
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("Failed to open strategy config: " + path);
  }
  std::ostringstream oss;
  oss << in.rdbuf();
  const std::string json = oss.str();

  StrategyConfig cfg;
  cfg.fee_price = ExtractDouble(json, "fee_price", cfg.fee_price);
  cfg.slip_price = ExtractDouble(json, "slip_price", cfg.slip_price);
  cfg.min_abs_direction_score =
      ExtractDouble(json, "min_abs_direction_score", cfg.min_abs_direction_score);
  cfg.min_expected_edge_bps =
      ExtractDouble(json, "min_expected_edge_bps", cfg.min_expected_edge_bps);
  cfg.max_mean_wait_ms =
      ExtractDouble(json, "max_mean_wait_ms", cfg.max_mean_wait_ms);
  return cfg;
}

// ------------------------- PnLAggregator -------------------------

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

  if (current_day_ == 0) {
    current_day_ = trade.day;
  } else if (trade.day != current_day_) {
    FlushCurrentDay();
    current_day_ = trade.day;
  }

  trades_.push_back(trade);

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

  DailyPnlRow row;
  row.day = current_day_;
  row.num_trades = day_trade_count_;
  row.gross_ret_sum = day_gross_sum_;
  row.net_ret_sum = day_net_sum_;
  row.gross_ret_mean = day_gross_sum_ / static_cast<double>(day_trade_count_);
  row.net_ret_mean = day_net_sum_ / static_cast<double>(day_trade_count_);
  row.cumulative_net_ret = cumulative_net_;

  daily_rows_.push_back(row);

  day_trade_count_ = 0;
  day_gross_sum_ = 0.0;
  day_net_sum_ = 0.0;
}

void PnLAggregator::WriteTradesCsv() const {
  std::filesystem::create_directories(trades_out_dir_);

  std::ostringstream fname;
  fname << "SPY_" << year_ << "_trades.csv";
  const std::string path = JoinPath(trades_out_dir_, fname.str());

  std::ofstream out(path);
  if (!out) {
    throw std::runtime_error("Failed to open trades output: " + path);
  }

  out << "ts_in,ts_out,day,mid_in,mid_out,spread_in,"
         "direction_score,expected_edge_ret,cost_ret,gross_ret,net_ret,side\n";

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

  std::ostringstream fname;
  fname << "SPY_" << year_ << "_daily.csv";
  const std::string path = JoinPath(daily_out_dir_, fname.str());

  std::ofstream out(path);
  if (!out) {
    throw std::runtime_error("Failed to open daily PnL output: " + path);
  }

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
  FlushCurrentDay();
  if (year_ == 0) return;
  WriteTradesCsv();
  WriteDailyCsv();
}

// ------------------------- Backtester -------------------------

Backtester::Backtester(const HistogramModel& hist,
                       const StrategyConfig& cfg,
                       std::string trades_out_dir,
                       std::string daily_out_dir)
    : hist_(hist),
      cfg_(cfg),
      pnl_(std::move(trades_out_dir), std::move(daily_out_dir)) {}

void Backtester::RunForYear(uint32_t year, const std::string& events_path) {
  // Read full per-event table into memory using Arrow/Parquet.
  std::shared_ptr<arrow::Schema> schema;
  std::unique_ptr<parquet::arrow::FileReader> reader =
    nbbo::open_parquet_reader(events_path, schema);
    
  std::shared_ptr<arrow::Table> table;
  {
  auto st = reader->ReadTable(&table);
  if (!st.ok()) {
      throw std::runtime_error("Failed to read events table from " + events_path +
                              ": " + st.ToString());
  }
  }

  auto combined_result = table->CombineChunks(arrow::default_memory_pool());
  if (!combined_result.ok()) {
  throw std::runtime_error("Failed to combine chunks for " + events_path +
                          ": " + combined_result.status().ToString());
  }
  std::shared_ptr<arrow::Table> combined = combined_result.ValueOrDie();

  const int ts_idx = schema->GetFieldIndex("ts");
  const int day_idx = schema->GetFieldIndex("day");
  const int mid_idx = schema->GetFieldIndex("mid");
  const int mid_next_idx = schema->GetFieldIndex("mid_next");
  const int spread_idx = schema->GetFieldIndex("spread");
  const int imb_idx = schema->GetFieldIndex("imbalance");
  const int age_idx = schema->GetFieldIndex("age_diff_ms");
  const int last_move_idx = schema->GetFieldIndex("last_move");
  const int y_idx = schema->GetFieldIndex("y");
  const int tau_idx = schema->GetFieldIndex("tau_ms");

  if (ts_idx < 0 || day_idx < 0 || mid_idx < 0 || mid_next_idx < 0 ||
      spread_idx < 0 || imb_idx < 0 || age_idx < 0 || last_move_idx < 0 ||
      y_idx < 0 || tau_idx < 0) {
    throw std::runtime_error("Expected LabeledEvent columns not found in schema");
  }

  auto ts_arr =
      std::static_pointer_cast<arrow::UInt64Array>(combined->column(ts_idx)->chunk(0));
  auto day_arr =
      std::static_pointer_cast<arrow::UInt32Array>(combined->column(day_idx)->chunk(0));
  auto mid_arr =
      std::static_pointer_cast<arrow::DoubleArray>(combined->column(mid_idx)->chunk(0));
  auto mid_next_arr = std::static_pointer_cast<arrow::DoubleArray>(
      combined->column(mid_next_idx)->chunk(0));
  auto spread_arr = std::static_pointer_cast<arrow::DoubleArray>(
      combined->column(spread_idx)->chunk(0));
  auto imb_arr =
      std::static_pointer_cast<arrow::DoubleArray>(combined->column(imb_idx)->chunk(0));
  auto age_arr =
      std::static_pointer_cast<arrow::DoubleArray>(combined->column(age_idx)->chunk(0));
  auto last_move_arr = std::static_pointer_cast<arrow::DoubleArray>(
      combined->column(last_move_idx)->chunk(0));
  auto y_arr =
      std::static_pointer_cast<arrow::DoubleArray>(combined->column(y_idx)->chunk(0));
  auto tau_arr =
      std::static_pointer_cast<arrow::DoubleArray>(combined->column(tau_idx)->chunk(0));

  const int64_t n = combined->num_rows();
  std::vector<nbbo::LabeledEvent> events;
  events.reserve(static_cast<std::size_t>(n));

  for (int64_t i = 0; i < n; ++i) {
    nbbo::LabeledEvent e{};
    e.ts = ts_arr->Value(i);
    e.day = day_arr->Value(i);
    e.mid = mid_arr->Value(i);
    e.mid_next = mid_next_arr->Value(i);
    e.spread = spread_arr->Value(i);
    e.imbalance = imb_arr->Value(i);
    e.age_diff_ms = age_arr->Value(i);
    e.last_move = last_move_arr->Value(i);
    e.y = y_arr->Value(i);
    e.tau_ms = tau_arr->Value(i);
    events.push_back(e);
  }

  pnl_.StartYear(year);

  for (std::size_t i = 0; i + 1 < events.size(); ++i) {
    const nbbo::LabeledEvent& ev = events[i];
    const nbbo::LabeledEvent& next = events[i + 1];

    const nbbo::LabeledEvent* next_ptr =
        (next.day == ev.day) ? &next : nullptr;
    ProcessEvent(ev, next_ptr);
  }

  // The very last event of the year cannot open a new trade sensibly.
  pnl_.FinalizeYear();
}

void Backtester::ProcessEvent(const nbbo::LabeledEvent& ev,
                              const nbbo::LabeledEvent* next_event) {
  if (!next_event) {
    return;
  }

  if (ev.mid <= 0.0 || ev.spread <= 0.0) {
    return;
  }

  TickState state{};
  state.imbalance   = ev.imbalance;
  state.spread      = ev.spread;
  state.age_diff_ms = ev.age_diff_ms;
  state.last_move   = ev.last_move;

  const double direction_score = hist_.direction_score(state);

  if (std::abs(direction_score) < cfg_.min_abs_direction_score) {
    return;
  }

  const double delta_m = 0.5 * ev.spread;
  const double expected_edge_ret = direction_score * (delta_m / ev.mid);

  const double c_spread = ev.spread / ev.mid;
  const double c_fee = 2.0 * cfg_.fee_price / ev.mid;
  const double c_slip = cfg_.slip_price / ev.mid;
  const double cost_ret = c_spread + c_fee + c_slip;

  const double margin_ret = cfg_.min_expected_edge_bps * 1e-4;
  if (expected_edge_ret <= cost_ret + margin_ret) {
    return;
  }

  if (cfg_.max_mean_wait_ms > 0.0) {
    const double mean_tau = hist_.mean_tau_ms(state);
    if (mean_tau > cfg_.max_mean_wait_ms) {
      return;
    }
  }

  const int side = (direction_score > 0.0) ? +1 : -1;

  const double gross_ret =
      side * ((next_event->mid - ev.mid) / ev.mid);
  const double net_ret = gross_ret - cost_ret;

  TradeRecord trade;
  trade.ts_in = ev.ts;
  trade.ts_out = next_event->ts;
  trade.day = ev.day;
  trade.mid_in = ev.mid;
  trade.mid_out = next_event->mid;
  trade.spread_in = ev.spread;
  trade.direction_score = direction_score;
  trade.expected_edge_ret = expected_edge_ret;
  trade.cost_ret = cost_ret;
  trade.gross_ret = gross_ret;
  trade.net_ret = net_ret;
  trade.side = side;

  pnl_.OnTrade(trade);
}

}  // namespace nbbo
