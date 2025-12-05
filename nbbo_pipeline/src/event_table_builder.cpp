

#include "nbbo/event_table_builder.hpp"

#include <cmath>
#include <filesystem>
#include <iostream>
#include <limits>
#include <stdexcept>
#include <vector>

#include "nbbo/arrow_utils.hpp"
#include "nbbo/time_utils.hpp"

namespace fs = std::filesystem;

EventTableBuilder::EventTableBuilder(const BuildEventsConfig& cfg)
    : cfg_(cfg), writer_(cfg.out_path) {}

void EventTableBuilder::run() {
  // High-level coordinator for building features (events):
  //   1. Ensure output directory exists
  //   2. Open parquet input (schema + reader)
  //   3. Build a streaming RecordBatchReader
  //   4. Stream and process ticks in batches
  //   5. Drop final day's unfinished event
  //   6. Close writer and print summary

  ensure_output_dir();
  open_input();
  init_reader();
  process_stream();
  finish_day();
  writer_.close();
  print_summary();
}

void EventTableBuilder::ensure_output_dir() {
  // Create parent directory of the output file
  try {
    fs::create_directories(fs::path(cfg_.out_path).parent_path());
  } catch (...) {
  }
}

void EventTableBuilder::open_input() {
  // Fetch schema from parquet reader
  reader_ = nbbo::open_parquet_reader(cfg_.in_path, in_schema_);
  if (!in_schema_) {
    throw std::runtime_error("input schema is null");
  }
}

void EventTableBuilder::init_reader() {
  // Build a RecordBatchReader over all rows + columns via batch streaming
  std::vector<int> all_row_groups;
  {
    auto* pq_reader = reader_->parquet_reader();
    int nrg = pq_reader->metadata()->num_row_groups();
    all_row_groups.reserve(nrg);
    for (int i = 0; i < nrg; ++i) all_row_groups.push_back(i);

    std::cout << "=== build_events ===\n";
    std::cout << "  in = " << cfg_.in_path << "\n";
    std::cout << "  out = " << cfg_.out_path << "\n";
    std::cout << "  threshold_next = " << cfg_.threshold_next << " (dollars)\n";
    std::cout << "  row_groups = " << nrg << "\n";
  }

  // Select all columns to read the full table
  std::vector<int> all_cols(in_schema_->num_fields());
  for (int i = 0; i < in_schema_->num_fields(); ++i) all_cols[i] = i;

  auto rb_res = reader_->GetRecordBatchReader(all_row_groups, all_cols);
  if (!rb_res.ok()) {
    throw std::runtime_error("GetRecordBatchReader failed: " +
                             rb_res.status().ToString());
  }
  rb_reader_ = std::move(rb_res).ValueOrDie();
}

void EventTableBuilder::process_stream() {
  // Main streaming loop to process in batches
  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    auto st = rb_reader_->ReadNext(&batch);
    if (!st.ok()) {
      throw std::runtime_error("ReadNext failed: " + st.ToString());
    }

    // EOF
    if (!batch) break;

    if (batch->num_rows() == 0) continue;

    process_batch(batch);
  }
}

void EventTableBuilder::process_batch(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  // Processes a batch iteratively per row to generate features (events)

  // Field validation. If column is missing, throws runtime error
  auto ts_arr = batch->GetColumnByName("ts");
  auto mid_arr = batch->GetColumnByName("mid");
  auto lr_arr = batch->GetColumnByName("log_return");
  auto bid_sz_arr = batch->GetColumnByName("bid_size");
  auto ask_sz_arr = batch->GetColumnByName("ask_size");
  auto spread_arr = batch->GetColumnByName("spread");
  auto bid_arr = batch->GetColumnByName("bid");
  auto ask_arr = batch->GetColumnByName("ask");

  if (!ts_arr || !mid_arr || !lr_arr || !bid_sz_arr || !ask_sz_arr ||
      !spread_arr || !bid_arr || !ask_arr) {
    throw std::runtime_error("Input batch missing required columns");
  }

  // Process each row sequentially in a batch
  const int64_t n = batch->num_rows();
  for (int64_t i = 0; i < n; ++i) {
    process_row(i, ts_arr, mid_arr, lr_arr, bid_sz_arr, ask_sz_arr, spread_arr,
                bid_arr, ask_arr);
  }
}

void EventTableBuilder::process_row(
    int64_t i,
    const std::shared_ptr<arrow::Array>& ts_arr,
    const std::shared_ptr<arrow::Array>& mid_arr,
    const std::shared_ptr<arrow::Array>& lr_arr,
    const std::shared_ptr<arrow::Array>& bid_sz_arr,
    const std::shared_ptr<arrow::Array>& ask_sz_arr,
    const std::shared_ptr<arrow::Array>& spread_arr,
    const std::shared_ptr<arrow::Array>& bid_arr,
    const std::shared_ptr<arrow::Array>& ask_arr) {
  // Count ticks for progress reporting
  ++ticks_total_;
  static constexpr int64_t PROGRESS_EVERY = 10'000'000;
  if (PROGRESS_EVERY > 0 && (ticks_total_ % PROGRESS_EVERY) == 0) {
    std::cout << "  processed ticks=" << ticks_total_
              << " events_written=" << events_written_ << "\n";
  }

  // null filtering. Everything except log_return must be non-null
  if (ts_arr->IsNull(i) || mid_arr->IsNull(i) || bid_sz_arr->IsNull(i) ||
      ask_sz_arr->IsNull(i) || spread_arr->IsNull(i) || bid_arr->IsNull(i) ||
      ask_arr->IsNull(i)) {
    return;
  }

  // Raw extract from Arrow arrays
  uint64_t ts = nbbo::ValueAt<uint64_t>(ts_arr, i);
  double mid = nbbo::ValueAt<double>(mid_arr, i);
  double bid = nbbo::ValueAt<double>(bid_arr, i);
  double ask = nbbo::ValueAt<double>(ask_arr, i);
  double bid_sz = nbbo::ValueAt<double>(bid_sz_arr, i);
  double ask_sz = nbbo::ValueAt<double>(ask_sz_arr, i);
  double spread = nbbo::ValueAt<double>(spread_arr, i);

  // log_return can be null. Treat null as "no mid change"
  double lr = std::numeric_limits<double>::quiet_NaN();
  if (!lr_arr->IsNull(i)) lr = nbbo::ValueAt<double>(lr_arr, i);

  // When the calendar day changes, the previous event must be dropped
  uint32_t day = nbbo::day_from_ts(ts);
  int ms = static_cast<int>(nbbo::ms_since_midnight_chrono(ts).count());
  if (!have_day_ || day != curr_day_) {
    start_new_day(day, ts, bid, ask);
  }

  // Calculate price imbalance and diff (ms) between bid and ask
  update_quote_ages(ms, bid, ask);
  double imbalance = compute_imbalance(bid_sz, ask_sz);
  double age_diff_ms = age_bid_ms_ - age_ask_ms_;

  // Only log_return != 0 marks a mid-price change event
  if (!std::isfinite(lr) || lr == 0.0) return;

  ++events_detected_;

  // Creates an event struct representing current mid-change
  nbbo::LabeledEvent event{};
  event.ts = ts;
  event.day = day;
  event.mid = mid;
  event.spread = spread;
  event.imbalance = imbalance;
  event.age_diff_ms = age_diff_ms;
  event.last_move = last_move_sign_;

  // Label previous event using the current one as "next mid change"
  label_and_emit_prev(event, ms);

  // Update last move sign for next event
  last_move_sign_ = (lr > 0.0 ? 1.0 : -1.0);

  // Store current event for labeling later
  prev_event_ = event;
  have_prev_event_ = true;
}

void EventTableBuilder::start_new_day(uint32_t day,
                                      uint64_t ts,
                                      double bid,
                                      double ask) {
  // Initialize state for a new trading day
  curr_day_ = day;
  have_day_ = true;

  // Reset quote age based on first tick of the day
  int ms = static_cast<int>(nbbo::ms_since_midnight_chrono(ts).count());
  last_bid_price_ = bid;
  last_ask_price_ = ask;
  bid_origin_ms_ = ms;
  ask_origin_ms_ = ms;
  last_move_sign_ = 0.0;

  // Leftover events from prior day do not have a "next" event
  if (have_prev_event_) {
    ++events_dropped_boundary_;
    have_prev_event_ = false;
  }
}

void EventTableBuilder::finish_day() {
  // If the final day has a pending event,
  // it is dropped because it has no next same-day mid-change
  if (have_prev_event_) {
    ++events_dropped_boundary_;
    have_prev_event_ = false;
  }
}

void EventTableBuilder::update_quote_ages(int ms, double bid, double ask) {
  // If price changes, update age. Else, age increases.

  if (bid != last_bid_price_) {
    last_bid_price_ = bid;
    bid_origin_ms_ = ms;
  }
  if (ask != last_ask_price_) {
    last_ask_price_ = ask;
    ask_origin_ms_ = ms;
  }
  age_bid_ms_ = ms - bid_origin_ms_;
  age_ask_ms_ = ms - ask_origin_ms_;
}

double EventTableBuilder::compute_imbalance(double bid_sz, double ask_sz) {
  // volume imbalance: (bid - ask) / (bid + ask)
  double denom = bid_sz + ask_sz;
  if (denom == 0.0) return 0.0;
  return (bid_sz - ask_sz) / denom;
}

void EventTableBuilder::label_and_emit_prev(const nbbo::LabeledEvent& event,
                                            int ms_curr) {
  // Label previous event only if a prev event exists
  // OR it's on same day as current event
  if (!have_prev_event_ || prev_event_.day != event.day) return;

  // Price movement between events
  double dmid = event.mid - prev_event_.mid;

  // mid jumps beyond threshold are considered outliers and dropped
  if (std::fabs(dmid) <= cfg_.threshold_next) {
    prev_event_.mid_next = event.mid;
    prev_event_.y = (dmid > 0.0 ? 1.0 : (dmid < 0.0 ? -1.0 : 0.0));

    // Waiting time until next event
    int ms_prev = static_cast<int>(
        nbbo::ms_since_midnight_chrono(prev_event_.ts).count());
    prev_event_.tau_ms = static_cast<double>(ms_curr - ms_prev);

    writer_.append(prev_event_);
    ++events_written_;
  } else {
    ++events_dropped_bigmove_;
  }
}

void EventTableBuilder::print_summary() const {
  std::cout << "=== summary ===\n";
  std::cout << "  ticks_total = " << ticks_total_ << "\n";
  std::cout << "  events_detected = " << events_detected_ << "\n";
  std::cout << "  events_written = " << events_written_ << "\n";
  std::cout << "  events_dropped_bigmove = " << events_dropped_bigmove_ << "\n";
  std::cout << "  events_dropped_boundary = " << events_dropped_boundary_
            << "\n";
}
