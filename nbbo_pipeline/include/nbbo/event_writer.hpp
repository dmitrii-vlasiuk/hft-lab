#pragma once

#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/writer.h>

#include <cstdint>
#include <memory>
#include <stdexcept>
#include <string>

#include "nbbo/arrow_utils.hpp"
#include "nbbo/event_types.hpp"

namespace nbbo {

// Writes nbbo::LabeledEvent rows into a parquet file
class EventWriter {
 public:
  explicit EventWriter(const std::string& out_path)
      : tsb_(arrow::default_memory_pool()),
        dateb_(arrow::default_memory_pool()),
        midb_(arrow::default_memory_pool()),
        mid_nextb_(arrow::default_memory_pool()),
        sprb_(arrow::default_memory_pool()),
        imbb_(arrow::default_memory_pool()),
        agediffb_(arrow::default_memory_pool()),
        lastmoveb_(arrow::default_memory_pool()),
        yb_(arrow::default_memory_pool()),
        taub_(arrow::default_memory_pool()) {
    schema_ = arrow::schema({
        arrow::field("ts", arrow::uint64()),
        arrow::field("date", arrow::uint32()),
        arrow::field("mid", arrow::float64()),
        arrow::field("mid_next", arrow::float64()),
        arrow::field("spread", arrow::float64()),
        arrow::field("imbalance", arrow::float64()),
        arrow::field("age_diff_ms", arrow::float64()),
        arrow::field("last_move", arrow::float64()),
        arrow::field("y", arrow::float64()),
        arrow::field("tau_ms", arrow::float64()),
    });

    // Open file output stream
    auto of_res = arrow::io::FileOutputStream::Open(out_path);
    if (!of_res.ok()) {
      throw std::runtime_error("open output failed: " +
                               of_res.status().ToString());
    }
    auto outfile = *of_res;

    // Create Parquet writer
    auto fw_res = parquet::arrow::FileWriter::Open(
        *schema_, arrow::default_memory_pool(), outfile);
    if (!fw_res.ok()) {
      throw std::runtime_error("create writer failed: " +
                               fw_res.status().ToString());
    }
    writer_ = std::move(fw_res).ValueOrDie();
  }

  void append(const nbbo::LabeledEvent& ev) {
    // Append one LabeledEvent to the active batch
    // Automatically triggers a batch flush once `BATCH` rows are buffered.
    nbbo::ARROW_OK(tsb_.Append(ev.ts));
    nbbo::ARROW_OK(dateb_.Append(ev.day));
    nbbo::ARROW_OK(midb_.Append(ev.mid));
    nbbo::ARROW_OK(mid_nextb_.Append(ev.mid_next));
    nbbo::ARROW_OK(sprb_.Append(ev.spread));
    nbbo::ARROW_OK(imbb_.Append(ev.imbalance));
    nbbo::ARROW_OK(agediffb_.Append(ev.age_diff_ms));
    nbbo::ARROW_OK(lastmoveb_.Append(ev.last_move));
    nbbo::ARROW_OK(yb_.Append(ev.y));
    nbbo::ARROW_OK(taub_.Append(ev.tau_ms));

    if (++batch_rows_ >= BATCH) {
      flush_batch();
    }
  }

  void close() {
    // Finalize file by:
    // - Flushing remaining buffered rows
    // - Closing the parquet writer
    flush_batch();
    if (writer_) {
      nbbo::ARROW_OK(writer_->Close());
    }
  }

  uint64_t total_rows() const { return total_rows_; }

 private:
  void flush_batch() {
    // Convert builders -> RecordBatch -> Parquet
    // Called automatically every `BATCH` rows or on close()
    if (batch_rows_ == 0) return;

    auto batch = arrow::RecordBatch::Make(schema_, batch_rows_,
                                          {
                                              tsb_.Finish().ValueOrDie(),
                                              dateb_.Finish().ValueOrDie(),
                                              midb_.Finish().ValueOrDie(),
                                              mid_nextb_.Finish().ValueOrDie(),
                                              sprb_.Finish().ValueOrDie(),
                                              imbb_.Finish().ValueOrDie(),
                                              agediffb_.Finish().ValueOrDie(),
                                              lastmoveb_.Finish().ValueOrDie(),
                                              yb_.Finish().ValueOrDie(),
                                              taub_.Finish().ValueOrDie(),
                                          });

    nbbo::ARROW_OK(writer_->WriteRecordBatch(*batch));
    total_rows_ += static_cast<uint64_t>(batch_rows_);
    batch_rows_ = 0;

    // Reset builders for next batch
    tsb_.Reset();
    dateb_.Reset();
    midb_.Reset();
    mid_nextb_.Reset();
    sprb_.Reset();
    imbb_.Reset();
    agediffb_.Reset();
    lastmoveb_.Reset();
    yb_.Reset();
    taub_.Reset();
  }

  // Flush interval
  static constexpr int64_t BATCH = 1'000'000;

  std::shared_ptr<arrow::Schema> schema_;
  std::unique_ptr<parquet::arrow::FileWriter> writer_;

  // Column builders
  arrow::UInt64Builder tsb_;
  arrow::UInt32Builder dateb_;
  arrow::DoubleBuilder midb_, mid_nextb_, sprb_, imbb_, agediffb_, lastmoveb_,
      yb_, taub_;

  int64_t batch_rows_ = 0;
  uint64_t total_rows_ = 0;
};

}  // namespace nbbo
