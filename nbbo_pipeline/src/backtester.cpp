// nbbo_pipeline/src/backtester.cpp
#include "nbbo/backtester.hpp"

#include <arrow/api.h>
#include <parquet/arrow/reader.h>

#include <cmath>
#include <memory>
#include <stdexcept>
#include <vector>

#include "nbbo/arrow_utils.hpp"

namespace nbbo {
  namespace {

    using UInt64Array = arrow::UInt64Array;
    using UInt32Array = arrow::UInt32Array;
    using DoubleArray = arrow::DoubleArray;

    // A small RAII wrapper that streams LabeledEvent rows from a Parquet file.
    //
    // Invariants:
    //  - Constructor verifies the schema has the expected columns and projects
    //    only those columns.
    //  - next(ev) returns true and fully-populates ev, or returns false at EOF.
    class LabeledEventStream {
    public:
      explicit LabeledEventStream(const std::string& events_path);

      // Fill ev with the next row; return false on EOF.
      bool next(LabeledEvent& ev);

    private:
      std::string path_;

      std::shared_ptr<arrow::Schema> schema_;
      std::unique_ptr<parquet::arrow::FileReader> reader_;
      std::unique_ptr<arrow::RecordBatchReader> batch_reader_;

      std::shared_ptr<arrow::RecordBatch> batch_;
      int64_t row_index_ = 0;
      int64_t row_count_ = 0;

      // Column views for the current batch (projected in a fixed order).
      std::shared_ptr<UInt64Array> ts_arr_;
      std::shared_ptr<UInt32Array> day_arr_;
      std::shared_ptr<DoubleArray> mid_arr_;
      std::shared_ptr<DoubleArray> mid_next_arr_;
      std::shared_ptr<DoubleArray> spread_arr_;
      std::shared_ptr<DoubleArray> imb_arr_;
      std::shared_ptr<DoubleArray> age_arr_;
      std::shared_ptr<DoubleArray> last_move_arr_;
      std::shared_ptr<DoubleArray> y_arr_;
      std::shared_ptr<DoubleArray> tau_arr_;

      void reset_batch();
      bool load_next_nonempty_batch();
    };

    LabeledEventStream::LabeledEventStream(const std::string& events_path)
        : path_(events_path) {
      // Open parquet reader and fetch schema (RAII: reader_ owns file resource).
      reader_ = open_parquet_reader(events_path, schema_);

      // Look up required columns by name in the schema.
      const int ts_idx        = schema_->GetFieldIndex("ts");
      const int day_idx       = schema_->GetFieldIndex("date");
      const int mid_idx       = schema_->GetFieldIndex("mid");
      const int mid_next_idx  = schema_->GetFieldIndex("mid_next");
      const int spread_idx    = schema_->GetFieldIndex("spread");
      const int imb_idx       = schema_->GetFieldIndex("imbalance");
      const int age_idx       = schema_->GetFieldIndex("age_diff_ms");
      const int last_move_idx = schema_->GetFieldIndex("last_move");
      const int y_idx         = schema_->GetFieldIndex("y");
      const int tau_idx       = schema_->GetFieldIndex("tau_ms");

      if (ts_idx < 0 || day_idx < 0 || mid_idx < 0 || mid_next_idx < 0 ||
          spread_idx < 0 || imb_idx < 0 || age_idx < 0 ||
          last_move_idx < 0 || y_idx < 0 || tau_idx < 0) {
        throw std::runtime_error(
            "Expected LabeledEvent columns not found in schema");
      }

      // Project only the columns we need, in a fixed order.
      std::vector<int> col_indices = {
          ts_idx, day_idx, mid_idx, mid_next_idx, spread_idx,
          imb_idx, age_idx, last_move_idx, y_idx, tau_idx};

      auto maybe_reader = reader_->GetRecordBatchReader(col_indices);
      if (!maybe_reader.ok()) {
        throw std::runtime_error(
            "Failed to create RecordBatchReader for " + events_path + ": " +
            maybe_reader.status().ToString());
      }
      batch_reader_ = std::move(maybe_reader).ValueOrDie();

      // Prime the first non-empty batch (if any).
      load_next_nonempty_batch();
    }

    void LabeledEventStream::reset_batch() {
      batch_.reset();
      row_index_ = 0;
      row_count_ = 0;

      ts_arr_.reset();
      day_arr_.reset();
      mid_arr_.reset();
      mid_next_arr_.reset();
      spread_arr_.reset();
      imb_arr_.reset();
      age_arr_.reset();
      last_move_arr_.reset();
      y_arr_.reset();
      tau_arr_.reset();
    }

    bool LabeledEventStream::load_next_nonempty_batch() {
      reset_batch();

      while (true) {
        std::shared_ptr<arrow::RecordBatch> next_batch;
        auto st = batch_reader_->ReadNext(&next_batch);
        if (!st.ok()) {
          throw std::runtime_error("Error reading batch from " + path_ +
                                   ": " + st.ToString());
        }

        if (!next_batch) {
          // EOF
          return false;
        }

        const int64_t n = next_batch->num_rows();
        if (n == 0) {
          // Skip empty batches.
          continue;
        }

        batch_     = std::move(next_batch);
        row_count_ = n;
        row_index_ = 0;

        // Columns come in the same order as col_indices in the constructor.
        ts_arr_        = std::static_pointer_cast<UInt64Array>(batch_->column(0));
        day_arr_       = std::static_pointer_cast<UInt32Array>(batch_->column(1));
        mid_arr_       = std::static_pointer_cast<DoubleArray>(batch_->column(2));
        mid_next_arr_  = std::static_pointer_cast<DoubleArray>(batch_->column(3));
        spread_arr_    = std::static_pointer_cast<DoubleArray>(batch_->column(4));
        imb_arr_       = std::static_pointer_cast<DoubleArray>(batch_->column(5));
        age_arr_       = std::static_pointer_cast<DoubleArray>(batch_->column(6));
        last_move_arr_ = std::static_pointer_cast<DoubleArray>(batch_->column(7));
        y_arr_         = std::static_pointer_cast<DoubleArray>(batch_->column(8));
        tau_arr_       = std::static_pointer_cast<DoubleArray>(batch_->column(9));
        return true;
      }
    }

    bool LabeledEventStream::next(LabeledEvent& ev) {
      if (row_index_ >= row_count_) {
        // Need a new batch; load_next_nonempty_batch may hit EOF.
        if (!load_next_nonempty_batch()) {
          return false;  // EOF
        }
      }

      const int64_t i = row_index_++;

      ev.ts          = ts_arr_->Value(i);
      ev.day         = day_arr_->Value(i);
      ev.mid         = mid_arr_->Value(i);
      ev.mid_next    = mid_next_arr_->Value(i);
      ev.spread      = spread_arr_->Value(i);
      ev.imbalance   = imb_arr_->Value(i);
      ev.age_diff_ms = age_arr_->Value(i);
      ev.last_move   = last_move_arr_->Value(i);
      ev.y           = y_arr_->Value(i);
      ev.tau_ms      = tau_arr_->Value(i);

      return true;
    }

  }  // namespace
}

namespace nbbo {
  // ------------------------- Backtester -------------------------
  //
  // Backtester:
  //   - Streams LabeledEvent rows from a Parquet file (Arrow)
  //   - For each pair of (current, next) events on the same day,
  //     calls ProcessEvent to decide whether to open a trade.
  //   - Hands all filled trades to PnLAggregator.
  //
  // edge_mode in StrategyConfig:
  //   0 = legacy:
  //         - no costs
  //         - gate on signed expected edge: expected_edge_ret > 0.0
  //   1 = Mode A (new trade-all):
  //         - costs ON (spread + fee + slip)
  //         - NO expected-edge gate; only min_abs_direction_score
  //   2 = Mode B (new edge-gated):
  //         - costs ON
  //         - |EE| > (fee + slip) + margin_ret, margin_ret from min_expected_edge_bps

  Backtester::Backtester(const HistogramModel& hist,
                        const StrategyConfig& cfg,
                        std::string trades_out_dir,
                        std::string daily_out_dir)
      : hist_(hist),
        cfg_(cfg),
        pnl_(std::move(trades_out_dir), std::move(daily_out_dir)) {}

  void Backtester::RunForYear(uint32_t year,
                            const std::string& events_path) {
    pnl_.StartYear(year);

    // RAII stream that owns the Arrow reader + record-batch reader.
    LabeledEventStream stream(events_path);

    LabeledEvent prev_ev{};
    LabeledEvent ev{};
    bool has_prev = false;

    while (stream.next(ev)) {
      // For each pair (prev_ev, ev) on the same day, treat ev as "next_event".
      if (has_prev) {
        const LabeledEvent* next_ptr =
            (ev.day == prev_ev.day) ? &ev : nullptr;
        ProcessEvent(prev_ev, next_ptr);
      }

      prev_ev = ev;
      has_prev = true;
    }

    // Last event of the year doesn't open a new trade (no "next" event).
    pnl_.FinalizeYear();
  }

  void Backtester::ProcessEvent(const LabeledEvent& ev,
                                const LabeledEvent* next_event) {
    // If there's no next event on the same day, we don't open a trade.
    if (!next_event) {
      return;
    }

    // Guard against bad data.
    if (ev.mid <= 0.0 || ev.spread <= 0.0) {
      return;
    }

    // Build tick state for histogram lookup.
    TickState state{};
    state.imbalance   = ev.imbalance;
    state.spread      = ev.spread;
    state.age_diff_ms = ev.age_diff_ms;
    state.last_move   = ev.last_move;

    // D(k): histogram-based direction score (signed).
    const double direction_score = hist_.direction_score(state);

    // Filter on direction score magnitude if requested.
    if (cfg_.min_abs_direction_score > 0.0 &&
        std::abs(direction_score) < cfg_.min_abs_direction_score) {
      return;
    }

    // Expected edge from histogram, in return space per $1 notional.
    // We approximate a one-tick mid move as spread / 2.
    const double delta_m = 0.5 * ev.spread;
    const double expected_edge_ret =
        direction_score * (delta_m / ev.mid);

    // Costs, initialized to zero so Legacy mode can keep them off.
    double c_spread = 0.0;
    double c_fee    = 0.0;
    double c_slip   = 0.0;
    double cost_ret = 0.0;

    // Shared cost computation for the “cost on” modes.
    auto compute_costs = [&] {
      c_spread = ev.spread / ev.mid;
      c_fee    = 2.0 * cfg_.fee_price / ev.mid;  // in/out legs
      c_slip   = cfg_.slip_price / ev.mid;
      cost_ret = c_spread + c_fee + c_slip;
    };

    // -------- EDGE-MODE SWITCH --------
    //
    // EdgeMode::Legacy:
    //   - no costs
    //   - gate on signed expected edge: expected_edge_ret > 0.0
    //
    // EdgeMode::CostTradeAll:
    //   - costs ON (spread + fee + slip)
    //   - NO expected-edge gate: trade-all as long as min_abs_direction_score passes
    //
    // EdgeMode::CostWithGate:
    //   - costs ON
    //   - absolute expected-edge gate:
    //        |EE| > (fee + slip) + margin_ret
    //     where margin_ret = min_expected_edge_bps * 1e-4
    switch (cfg_.edge_mode) {
      case EdgeMode::Legacy: {
        // Costs remain zero; gate on signed expected edge.
        if (expected_edge_ret <= 0.0) {
          return;
        }
        break;
      }

      case EdgeMode::CostTradeAll: {
        // Turn on realistic costs; no extra EE gate beyond direction filter.
        compute_costs();
        break;
      }

      case EdgeMode::CostWithGate: {
        // Turn on realistic costs.
        compute_costs();

        if (cfg_.min_expected_edge_bps > 0.0) {
          // Mode B only: expected-edge gate using absolute EE
          const double margin_ret =
              cfg_.min_expected_edge_bps * 1e-4;  // bps -> return
          const double cost_ret_gate = c_fee + c_slip;
          const double edge_ret_mag  = std::abs(expected_edge_ret);

          if (edge_ret_mag <= cost_ret_gate + margin_ret) {
            return;
          }
        }
        break;
      }

      default:
        throw std::logic_error("Unknown EdgeMode in Backtester::ProcessEvent");
    }

    // Optional wait-time filter from histogram: if expected time to
    // realize the move is too long, we skip the trade.
    if (cfg_.max_mean_wait_ms > 0.0) {
      const double mean_tau = hist_.mean_tau_ms(state);
      if (mean_tau > cfg_.max_mean_wait_ms) {
        return;
      }
    }

    // Trade direction from sign of signal.
    //   side = +1 -> long
    //   side = -1 -> short
    const int side = (direction_score > 0.0) ? +1 : -1;

    // Realized price move over one step, converted to return.
    const double gross_ret =
        side * ((next_event->mid - ev.mid) / ev.mid);

    // Net return after applying all costs in return space.
    const double net_ret = gross_ret - cost_ret;

    // Fill TradeRecord for downstream CSV aggregation.
    TradeRecord trade{};
    trade.ts_in             = ev.ts;
    trade.ts_out            = next_event->ts;
    trade.day               = ev.day;
    trade.mid_in            = ev.mid;
    trade.mid_out           = next_event->mid;
    trade.spread_in         = ev.spread;
    trade.direction_score   = direction_score;
    trade.expected_edge_ret = expected_edge_ret;
    trade.cost_ret          = cost_ret;
    trade.gross_ret         = gross_ret;
    trade.net_ret           = net_ret;
    trade.side              = side;

    pnl_.OnTrade(trade);
  }
}  // namespace nbbo
