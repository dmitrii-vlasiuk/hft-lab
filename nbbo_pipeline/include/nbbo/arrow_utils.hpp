#pragma once
#include <arrow/api.h>
#include <arrow/io/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <memory>
#include <stdexcept>
#include <string>

namespace nbbo {

// Generic declaration for typed value extraction from Arrow arrays
template <typename T>
T ValueAt(const std::shared_ptr<arrow::Array>& arr, int64_t i);

// Specialization for extracting timestamps as uint64_t
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
      throw std::runtime_error("Unsupported type: " + arr->type()->ToString());
  }
}

// Specialization for extracting numeric columns as double or float
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
      throw std::runtime_error("Unsupported type: " + arr->type()->ToString());
  }
}

inline std::unique_ptr<parquet::arrow::FileReader> open_parquet_reader(
    const std::string& path, std::shared_ptr<arrow::Schema>& out_schema) {
  // Open a Parquet file and return a FileReader

  // Open file
  auto readable_file_result = arrow::io::ReadableFile::Open(path);
  if (!readable_file_result.ok()) {
    throw std::runtime_error("open input failed: " +
                             readable_file_result.status().ToString());
  }

  // Open file via parquet reader
  auto parquet_read_result = parquet::arrow::OpenFile(
      *readable_file_result, arrow::default_memory_pool());
  if (!parquet_read_result.ok()) {
    throw std::runtime_error("open parquet reader failed: " +
                             parquet_read_result.status().ToString());
  }

  // Validate schema then return reader
  auto reader = std::move(parquet_read_result).ValueOrDie();
  auto st = reader->GetSchema(&out_schema);
  if (!st.ok()) {
    throw std::runtime_error("get schema failed: " + st.ToString());
  }
  return reader;
}

// helper used at runtime for validation
static inline void ARROW_OK(const arrow::Status& st) {
  if (!st.ok()) throw std::runtime_error(st.ToString());
}

}  // namespace nbbo
