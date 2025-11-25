#pragma once
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <memory>
#include <stdexcept>
#include <string>

namespace nbbo {

template <typename T>
T ValueAt(const std::shared_ptr<arrow::Array>& arr, int64_t i);

template <>
inline uint64_t ValueAt<uint64_t>(const std::shared_ptr<arrow::Array>& arr,
                                  int64_t i) {
  switch (arr->type_id()) {
    case arrow::Type::UINT64:
      return static_cast<const arrow::UInt64Array&>(*arr).Value(i);
    case arrow::Type::INT64:
      return static_cast<uint64_t>(
          static_cast<const arrow::Int64Array&>(*arr).Value(i));
    default:
      throw std::runtime_error("Unsupported ts type: " +
                               arr->type()->ToString());
  }
}

template <>
inline double ValueAt<double>(const std::shared_ptr<arrow::Array>& arr,
                              int64_t i) {
  switch (arr->type_id()) {
    case arrow::Type::FLOAT:
      return static_cast<double>(
          static_cast<const arrow::FloatArray&>(*arr).Value(i));
    case arrow::Type::DOUBLE:
      return static_cast<const arrow::DoubleArray&>(*arr).Value(i);
    default:
      throw std::runtime_error("Unsupported mid type: " +
                               arr->type()->ToString());
  }
}

// helper to open parquet FileReader
inline std::unique_ptr<parquet::arrow::FileReader> open_parquet_reader(
    const std::string& path, std::shared_ptr<arrow::Schema>& out_schema) {
  auto rf_res = arrow::io::ReadableFile::Open(path);
  if (!rf_res.ok()) {
    throw std::runtime_error("open input failed: " +
                             rf_res.status().ToString());
  }
  auto fr_res = parquet::arrow::OpenFile(*rf_res, arrow::default_memory_pool());
  if (!fr_res.ok()) {
    throw std::runtime_error("open parquet reader failed: " +
                             fr_res.status().ToString());
  }
  auto reader = std::move(fr_res).ValueOrDie();
  auto st = reader->GetSchema(&out_schema);
  if (!st.ok()) {
    throw std::runtime_error("get schema failed: " + st.ToString());
  }
  return reader;
}

}  // namespace nbbo
