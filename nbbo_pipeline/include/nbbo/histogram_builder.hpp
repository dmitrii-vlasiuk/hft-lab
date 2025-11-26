#pragma once
#include <memory>
#include <string>
#include <vector>

#include "nbbo/histogram_model.hpp"

namespace arrow {
class RecordBatch;
}

struct HistogramConfig {
  std::string events_root;  // e.g. "data/research/events"
  std::string symbol;       // "SPY"
  int year_lo;              // 2018
  int year_hi;              // 2022 (inclusive)
  std::string out_path;     // "data/research/hist/SPY_histogram.json"
  double alpha = 1.0;
};

class HistogramBuilder {
 public:
  explicit HistogramBuilder(const HistogramConfig& cfg);

  // Stream over events files and write JSON
  void run();

 private:
  HistogramConfig cfg_;
  HistogramModel hist_;

  void accumulate_year(int year);
  void accumulate_batch(const std::shared_ptr<arrow::RecordBatch>& batch);
  void finalize_and_write_json() const;
};
