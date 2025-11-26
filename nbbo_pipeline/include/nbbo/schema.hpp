#pragma once
#include <arrow/api.h>

#include <memory>

namespace nbbo {

inline std::shared_ptr<arrow::Schema> nbbo_schema() {
  return arrow::schema({
      arrow::field("ts", arrow::uint64()),
      arrow::field("mid", arrow::float32()),
      arrow::field("log_return", arrow::float32()),
      arrow::field("bid_size", arrow::float32()),
      arrow::field("ask_size", arrow::float32()),
      arrow::field("spread", arrow::float32()),
      arrow::field("bid", arrow::float32()),
      arrow::field("ask", arrow::float32()),
  });
}

}  // namespace nbbo
