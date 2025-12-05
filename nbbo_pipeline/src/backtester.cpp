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
  // Open Parquet reader and schema
  std::shared_ptr<arrow::Schema> schema;
  std::unique_ptr<parquet::arrow::FileReader> reader =
      nbbo::open_parquet_reader(events_path, schema);

  // Look up required columns by name in the schema.
  const int ts_idx        = schema->GetFieldIndex("ts");
  const int day_idx       = schema->GetFieldIndex("date");
  const int mid_idx       = schema->GetFieldIndex("mid");
  const int mid_next_idx  = schema->GetFieldIndex("mid_next");
  const int spread_idx    = schema->GetFieldIndex("spread");
  const int imb_idx       = schema->GetFieldIndex("imbalance");
  const int age_idx       = schema->GetFieldIndex("age_diff_ms");
  const int last_move_idx = schema->GetFieldIndex("last_move");
  const int y_idx         = schema->GetFieldIndex("y");
  const int tau_idx       = schema->GetFieldIndex("tau_ms");

  if (ts_idx < 0 || day_idx < 0 || mid_idx < 0 || mid_next_idx < 0 ||
      spread_idx < 0 || imb_idx < 0 || age_idx < 0 ||
      last_move_idx < 0 || y_idx < 0 || tau_idx < 0) {
    throw std::runtime_error(
        "Expected LabeledEvent columns not found in schema");
  }

  // We only need these 10 columns; tell Arrow to project just these.
  std::vector<int> col_indices = {
      ts_idx, day_idx, mid_idx, mid_next_idx, spread_idx,
      imb_idx, age_idx, last_move_idx, y_idx, tau_idx};

  // Modern GetRecordBatchReader API: returns Result<unique_ptr<RecordBatchReader>>.
  auto maybe_reader = reader->GetRecordBatchReader(col_indices);
  if (!maybe_reader.ok()) {
    throw std::runtime_error(
        "Failed to create RecordBatchReader for " + events_path + ": " +
        maybe_reader.status().ToString());
  }
  std::unique_ptr<arrow::RecordBatchReader> batch_reader =
      std::move(maybe_reader).ValueOrDie();

  pnl_.StartYear(year);

  LabeledEvent prev_ev{};
  bool has_prev = false;

  // Stream over record batches to avoid loading entire table into memory.
  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    auto st = batch_reader->ReadNext(&batch);
    if (!st.ok()) {
      throw std::runtime_error("Error reading batch from " + events_path +
                               ": " + st.ToString());
    }
    if (!batch) {
      break;  // no more batches
    }

    const int64_t n = batch->num_rows();
    if (n == 0) continue;

    // Columns come in the same order as col_indices above.
    auto ts_arr =
        std::static_pointer_cast<arrow::UInt64Array>(batch->column(0));
    auto day_arr =
        std::static_pointer_cast<arrow::UInt32Array>(batch->column(1));
    auto mid_arr =
        std::static_pointer_cast<arrow::DoubleArray>(batch->column(2));
    auto mid_next_arr =
        std::static_pointer_cast<arrow::DoubleArray>(batch->column(3));
    auto spread_arr =
        std::static_pointer_cast<arrow::DoubleArray>(batch->column(4));
    auto imb_arr =
        std::static_pointer_cast<arrow::DoubleArray>(batch->column(5));
    auto age_arr =
        std::static_pointer_cast<arrow::DoubleArray>(batch->column(6));
    auto last_move_arr =
        std::static_pointer_cast<arrow::DoubleArray>(batch->column(7));
    auto y_arr =
        std::static_pointer_cast<arrow::DoubleArray>(batch->column(8));
    auto tau_arr =
        std::static_pointer_cast<arrow::DoubleArray>(batch->column(9));

    for (int64_t i = 0; i < n; ++i) {
      // Build a LabeledEvent from the Arrow row.
      LabeledEvent ev{};
      ev.ts          = ts_arr->Value(i);
      ev.day         = day_arr->Value(i);
      ev.mid         = mid_arr->Value(i);
      ev.mid_next    = mid_next_arr->Value(i);
      ev.spread      = spread_arr->Value(i);
      ev.imbalance   = imb_arr->Value(i);
      ev.age_diff_ms = age_arr->Value(i);
      ev.last_move   = last_move_arr->Value(i);
      ev.y           = y_arr->Value(i);
      ev.tau_ms      = tau_arr->Value(i);

      // For each pair (prev_ev, ev) on the same day, treat ev as "next_event".
      if (has_prev) {
        const LabeledEvent* next_ptr =
            (ev.day == prev_ev.day) ? &ev : nullptr;
        ProcessEvent(prev_ev, next_ptr);
      }

      prev_ev = ev;
      has_prev = true;
    }
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

  // Basic signal strength gate (can be set to 0.0 in config to disable entirely).
  if (std::abs(direction_score) < cfg_.min_abs_direction_score) {
    return;
  }

  // Expected edge from histogram, in return space per $1 notional.
  // We approximate a one-tick mid move as spread / 2.
  const double delta_m = 0.5 * ev.spread;
  const double expected_edge_ret =
      direction_score * (delta_m / ev.mid);

  // Costs, initialized to zero so legacy mode can keep them off.
  double c_spread = 0.0;
  double c_fee    = 0.0;
  double c_slip   = 0.0;
  double cost_ret = 0.0;

  // -------- EDGE-MODE SWITCH --------
  //
  // edge_mode:
  //   0 = legacy:
  //         - no costs
  //         - gate on signed expected edge: expected_edge_ret > 0.0
  //
  //   1 = Mode A (new trade-all):
  //         - costs ON (spread + fee + slip)
  //         - NO expected-edge gate: trade-all as long as min_abs_direction_score passes
  //
  //   2 = Mode B (new edge gate):
  //         - costs ON
  //         - absolute expected-edge gate:
  //              |EE| > (fee + slip) + margin_ret
  //           where margin_ret = min_expected_edge_bps * 1e-4
  //
  if (cfg_.edge_mode == 0) {
    // ---------- LEGACY MODE ----------
    // Costs remain zero; gate on signed expected edge.
    if (expected_edge_ret <= 0.0) {
      return;
    }
  } else {
    // ---------- NEW MODES (1 & 2) ----------
    // Turn on realistic costs.
    c_spread = ev.spread / ev.mid;
    c_fee    = 2.0 * cfg_.fee_price / ev.mid;
    c_slip   = cfg_.slip_price / ev.mid;
    cost_ret = c_spread + c_fee + c_slip;

    if (cfg_.edge_mode == 2 && cfg_.min_expected_edge_bps > 0.0) {
      // Mode B only: expected-edge gate using absolute EE
      const double margin_ret =
          cfg_.min_expected_edge_bps * 1e-4;  // bps -> return
      const double cost_ret_gate = c_fee + c_slip;
      const double edge_ret_mag  = std::abs(expected_edge_ret);

      if (edge_ret_mag <= cost_ret_gate + margin_ret) {
        return;
      }
    }
    // Mode 1: no EE gate; just costs and min_abs_direction_score.
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
  TradeRecord trade;
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
