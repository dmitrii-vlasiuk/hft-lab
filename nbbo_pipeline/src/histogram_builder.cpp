#include "nbbo/histogram_builder.hpp"

#include <arrow/api.h>
#include <parquet/arrow/reader.h>

#include <cmath>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <stdexcept>
#include <string>
#include <vector>

#include "nbbo/arrow_utils.hpp"

namespace fs = std::filesystem;

HistogramBuilder::HistogramBuilder(const HistogramConfig& cfg) : cfg_(cfg) {
  hist_.alpha = cfg_.alpha;
}

void HistogramBuilder::run() {
  if (cfg_.year_hi < cfg_.year_lo) {
    throw std::runtime_error("HistogramBuilder: year_hi < year_lo");
  }

  std::cout << "=== build_histogram ===\n";
  std::cout << "  symbol = " << cfg_.symbol << "\n";
  std::cout << "  events_root = " << cfg_.events_root << "\n";
  std::cout << "  years = " << cfg_.year_lo << ":" << cfg_.year_hi << "\n";
  std::cout << "  out = " << cfg_.out_path << "\n";
  std::cout << "  alpha = " << cfg_.alpha << "\n";

  for (int y = cfg_.year_lo; y <= cfg_.year_hi; ++y) {
    accumulate_year(y);
  }

  finalize_and_write_json();
}

void HistogramBuilder::accumulate_year(int year) {
  fs::path in_path =
      fs::path(cfg_.events_root) /
      (cfg_.symbol + "_" + std::to_string(year) + "_events.parquet");

  std::cout << "  [year " << year << "] reading " << in_path.string() << "\n";

  std::shared_ptr<arrow::Schema> schema;
  auto reader = nbbo::open_parquet_reader(in_path.string(), schema);
  if (!schema) {
    throw std::runtime_error("HistogramBuilder: input schema is null");
  }

  // Row groups: all
  auto* pq_reader = reader->parquet_reader();
  int nrg = pq_reader->metadata()->num_row_groups();
  std::vector<int> all_row_groups;
  all_row_groups.reserve(nrg);
  for (int i = 0; i < nrg; ++i) all_row_groups.push_back(i);

  // Columns: all
  std::vector<int> all_cols(schema->num_fields());
  for (int i = 0; i < schema->num_fields(); ++i) all_cols[i] = i;

  auto rb_res = reader->GetRecordBatchReader(all_row_groups, all_cols);
  if (!rb_res.ok()) {
    throw std::runtime_error("GetRecordBatchReader failed: " +
                             rb_res.status().ToString());
  }
  auto rb_reader = std::move(rb_res).ValueOrDie();

  while (true) {
    std::shared_ptr<arrow::RecordBatch> batch;
    auto st = rb_reader->ReadNext(&batch);
    if (!st.ok()) {
      throw std::runtime_error("ReadNext failed: " + st.ToString());
    }
    if (!batch) break;
    if (batch->num_rows() == 0) continue;

    accumulate_batch(batch);
  }
}

void HistogramBuilder::accumulate_batch(
    const std::shared_ptr<arrow::RecordBatch>& batch) {
  auto imb_arr = batch->GetColumnByName("imbalance");
  auto spr_arr = batch->GetColumnByName("spread");
  auto age_arr = batch->GetColumnByName("age_diff_ms");
  auto last_arr = batch->GetColumnByName("last_move");
  auto y_arr = batch->GetColumnByName("y");
  auto tau_arr = batch->GetColumnByName("tau_ms");

  if (!imb_arr || !spr_arr || !age_arr || !last_arr || !y_arr || !tau_arr) {
    throw std::runtime_error(
        "HistogramBuilder: events batch missing required columns");
  }

  const int64_t n = batch->num_rows();
  for (int64_t i = 0; i < n; ++i) {
    // Skip rows with any null in required columns
    if (imb_arr->IsNull(i) || spr_arr->IsNull(i) || age_arr->IsNull(i) ||
        last_arr->IsNull(i) || y_arr->IsNull(i) || tau_arr->IsNull(i)) {
      continue;
    }

    TickState x{
        nbbo::ValueAt<double>(imb_arr, i),  // imbalance
        nbbo::ValueAt<double>(spr_arr, i),  // spread
        nbbo::ValueAt<double>(age_arr, i),  // age_diff_ms
        nbbo::ValueAt<double>(last_arr, i)  // last_move
    };
    double Y = nbbo::ValueAt<double>(y_arr, i);
    double tau = nbbo::ValueAt<double>(tau_arr, i);

    int k = hist_.cell_index(x);
    CellStats& cs = hist_.cells[static_cast<std::size_t>(k)];

    cs.n += 1;
    if (Y > 0.0) {
      cs.n_up += 1;
    } else if (Y < 0.0) {
      cs.n_down += 1;
    }
    cs.sum_tau_ms += tau;
  }
}

void HistogramBuilder::finalize_and_write_json() const {
  fs::path out_path(cfg_.out_path);
  if (!out_path.parent_path().empty()) {
    try {
      fs::create_directories(out_path.parent_path());
    } catch (...) {
    }
  }

  std::ofstream ofs(out_path);
  if (!ofs.is_open()) {
    throw std::runtime_error("HistogramBuilder: cannot open output: " +
                             out_path.string());
  }

  ofs << std::setprecision(10);

  // Compute global tau statistics for empty-cell fallback
  double global_sum_tau = 0.0;
  std::uint64_t global_n = 0;
  for (int k = 0; k < HistogramModel::N_CELLS; ++k) {
    const CellStats& c = hist_.cells[static_cast<std::size_t>(k)];
    global_sum_tau += c.sum_tau_ms;
    global_n += c.n;
  }
  double global_mean_tau = 0.0;
  if (global_n > 0) {
    global_mean_tau = global_sum_tau / static_cast<double>(global_n);
  }
  // Large sentinel
  double tau_fallback = (global_n > 0) ? (2.0 * global_mean_tau) : 0.0;

  // JSON header and bin definitions
  ofs << "{\n";
  ofs << "  \"symbol\": \"" << cfg_.symbol << "\",\n";
  ofs << "  \"year_lo\": " << cfg_.year_lo << ",\n";
  ofs << "  \"year_hi\": " << cfg_.year_hi << ",\n";
  ofs << "  \"alpha\": " << hist_.alpha << ",\n";

  // Imbalance bins: 6 bins on [-1,1]
  ofs << "  \"imbalance_bins\": [\n";
  const char* imb_bin_str[HistogramModel::N_IMB] = {
      "[-1.0, -0.7)", "[-0.7, -0.3)", "[-0.3, -0.1)",
      "[-0.1, 0.1]",  "(0.1, 0.3]",   "(0.3, 1.0]"};
  for (int b = 0; b < HistogramModel::N_IMB; ++b) {
    double lo = 0.0, hi = 0.0;
    switch (b) {
      case 0:
        lo = -1.0;
        hi = -0.7;
        break;
      case 1:
        lo = -0.7;
        hi = -0.3;
        break;
      case 2:
        lo = -0.3;
        hi = -0.1;
        break;
      case 3:
        lo = -0.1;
        hi = 0.1;
        break;
      case 4:
        lo = 0.1;
        hi = 0.3;
        break;
      case 5:
        lo = 0.3;
        hi = 1.0;
        break;
      default:
        lo = 0.0;
        hi = 0.0;
        break;
    }
    ofs << "    {\"idx\": " << b << ", \"lo\": " << lo << ", \"hi\": " << hi
        << ", \"interval\": \"" << imb_bin_str[b] << "\"}";
    if (b + 1 < HistogramModel::N_IMB) ofs << ",";
    ofs << "\n";
  }
  ofs << "  ],\n";

  // Spread bins: based on tick count k = round(spread / 0.01)
  // bin 0: k <= 1, bin 1: k == 2, bin 2: k >= 3
  ofs << "  \"spread_bins\": [\n";
  ofs << "    {\"idx\": 0, \"ticks_min\": 0, \"ticks_max\": 1},\n";
  ofs << "    {\"idx\": 1, \"ticks_min\": 2, \"ticks_max\": 2},\n";
  ofs << "    {\"idx\": 2, \"ticks_min\": 3, \"ticks_max\": null}\n";
  ofs << "  ],\n";

  // Age-diff bins: (-inf,-200), [-200,-50), [-50,50], (50,200], (200,inf)
  ofs << "  \"age_diff_ms_bins\": [\n";
  ofs << "    {\"idx\": 0, \"lo\": null,   \"hi\": -200.0},\n";
  ofs << "    {\"idx\": 1, \"lo\": -200.0, \"hi\": -50.0},\n";
  ofs << "    {\"idx\": 2, \"lo\": -50.0,  \"hi\": 50.0},\n";
  ofs << "    {\"idx\": 3, \"lo\": 50.0,   \"hi\": 200.0},\n";
  ofs << "    {\"idx\": 4, \"lo\": 200.0,  \"hi\": null}\n";
  ofs << "  ],\n";

  // Last-move bins: {-1, 0, +1}
  ofs << "  \"last_move_bins\": [\n";
  ofs << "    {\"idx\": 0, \"L\": -1},\n";
  ofs << "    {\"idx\": 1, \"L\": 0},\n";
  ofs << "    {\"idx\": 2, \"L\": 1}\n";
  ofs << "  ],\n";

  // 3) Cells with bin indices and derived stats
  ofs << "  \"cells\": [\n";

  for (int k = 0; k < HistogramModel::N_CELLS; ++k) {
    const CellStats& c = hist_.cells[static_cast<std::size_t>(k)];

    // decode (b_imb, b_spr, b_age, b_last) from linear index k
    int tmp = k;
    int b_last = tmp % HistogramModel::N_LAST;
    tmp /= HistogramModel::N_LAST;
    int b_age = tmp % HistogramModel::N_AGE;
    tmp /= HistogramModel::N_AGE;
    int b_spr = tmp % HistogramModel::N_SPR;
    tmp /= HistogramModel::N_SPR;
    int b_imb = tmp;  // remaining

    double p_up = hist_.p_up(k);
    double p_down = hist_.p_down(k);
    double D = hist_.direction_score(k);
    double mean_tau = hist_.mean_tau_ms(k);
    if (!std::isfinite(mean_tau)) {
      mean_tau = tau_fallback;  // empty-cell fallback
    }

    ofs << "    {"
        << "\"idx\": " << k << ", \"b_imb\": " << b_imb
        << ", \"b_spr\": " << b_spr << ", \"b_age\": " << b_age
        << ", \"b_last\": " << b_last << ", \"n\": " << c.n
        << ", \"n_up\": " << c.n_up << ", \"n_down\": " << c.n_down
        << ", \"sum_tau_ms\": " << c.sum_tau_ms << ", \"p_up\": " << p_up
        << ", \"p_down\": " << p_down << ", \"D\": " << D
        << ", \"mean_tau_ms\": " << mean_tau << "}";

    if (k + 1 < HistogramModel::N_CELLS) ofs << ",";
    ofs << "\n";
  }

  ofs << "  ]\n";
  ofs << "}\n";

  std::cout << "  wrote histogram JSON to " << out_path.string() << "\n";
}
