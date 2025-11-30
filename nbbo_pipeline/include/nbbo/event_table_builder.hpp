#pragma once

#include <arrow/api.h>
#include <parquet/arrow/reader.h>

#include <cstdint>
#include <memory>

#include "nbbo/build_events_config.hpp"
#include "nbbo/event_types.hpp"
#include "nbbo/event_writer.hpp"

// builds per-mid-change events and labels them with next move and waiting time
// from a cleaned NBBO grid
class EventTableBuilder {
 public:
  explicit EventTableBuilder(const BuildEventsConfig& cfg);

  // entry point to run pipeline from input file to labeled events output file
  void run();

 private:
  void ensure_output_dir();
  void open_input();
  void init_reader();
  void process_stream();

  void process_batch(const std::shared_ptr<arrow::RecordBatch>& batch);

  void process_row(int64_t i,
                   const std::shared_ptr<arrow::Array>& ts_arr,
                   const std::shared_ptr<arrow::Array>& mid_arr,
                   const std::shared_ptr<arrow::Array>& lr_arr,
                   const std::shared_ptr<arrow::Array>& bid_sz_arr,
                   const std::shared_ptr<arrow::Array>& ask_sz_arr,
                   const std::shared_ptr<arrow::Array>& spread_arr,
                   const std::shared_ptr<arrow::Array>& bid_arr,
                   const std::shared_ptr<arrow::Array>& ask_arr);

  void start_new_day(uint32_t day, uint64_t ts, double bid, double ask);
  void finish_day();

  void update_quote_ages(int ms, double bid, double ask);

  static double compute_imbalance(double bid_sz, double ask_sz);

  void label_and_emit_prev(const nbbo::LabeledEvent& ev, int ms_curr);
  void print_summary() const;

  BuildEventsConfig cfg_;
  std::shared_ptr<arrow::Schema> in_schema_;
  std::unique_ptr<parquet::arrow::FileReader> reader_;
  std::shared_ptr<arrow::RecordBatchReader> rb_reader_;
  nbbo::EventWriter writer_;

  uint64_t ticks_total_ = 0;
  uint64_t events_detected_ = 0;
  uint64_t events_written_ = 0;
  uint64_t events_dropped_bigmove_ = 0;
  uint64_t events_dropped_boundary_ = 0;

  uint32_t curr_day_ = 0;
  bool have_day_ = false;
  double last_bid_price_ = 0.0;
  double last_ask_price_ = 0.0;
  int bid_origin_ms_ = 0;
  int ask_origin_ms_ = 0;
  double age_bid_ms_ = 0.0;
  double age_ask_ms_ = 0.0;

  double last_move_sign_ = 0.0;
  bool have_prev_event_ = false;
  nbbo::LabeledEvent prev_event_{};
};
