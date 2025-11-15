#include <arrow/api.h>
#include <arrow/io/api.h>
#include <arrow/compute/api.h>
#include <parquet/arrow/reader.h>
#include <parquet/arrow/writer.h>

#include <cmath>
#include <cstdint>
#include <cstdio>
#include <cstdlib>
#include <cstring>
#include <string>
#include <vector>
#include <unordered_map>
#include <memory>
#include <iostream>
#include <filesystem>
#include <algorithm>

using arrow::Status;
using arrow::Result;

static inline uint32_t day_from_ts(uint64_t ts) {
    // ts like yyyymmddHHMMSSmmm... -> day key yyyymmdd
    return static_cast<uint32_t>(ts / 1000000000ULL);
}

static inline std::string day_to_string(uint32_t d) {
    uint32_t y = d / 10000U;
    uint32_t m = (d / 100U) % 100U;
    uint32_t dd = d % 100U;
    char buf[16];
    std::snprintf(buf, sizeof(buf), "%04u-%02u-%02u", y, m, dd);
    return std::string(buf);
}

static void usage_and_exit(const char* argv0) {
    std::fprintf(stderr,
R"(Usage:
  %s --in <input.parquet> --out <output.parquet> [--thr <dollars>] [--progress <rows>]

Description:
  Removes intra-day mid-price jumps with |Δmid| >= threshold (default 100)
  and any rows where the mid price itself exceeds 1000.
  The Δmid is computed versus the last *kept* tick within the same day.
  First tick of each day is always tested only against the level filter;
  inter-day jumps are allowed.

Example:
  %s --in data/out/event/SPY_2020.parquet \
     --out data/out/event_clean_thr100/SPY_2020.parquet --thr 100
)",
    argv0, argv0);
    std::exit(2);
}

template <typename T>
static T ValueAt(const std::shared_ptr<arrow::Array>& arr, int64_t i);

template <>
uint64_t ValueAt<uint64_t>(const std::shared_ptr<arrow::Array>& arr, int64_t i) {
    switch (arr->type_id()) {
        case arrow::Type::UINT64: return static_cast<const arrow::UInt64Array&>(*arr).Value(i);
        case arrow::Type::INT64:  return static_cast<uint64_t>(static_cast<const arrow::Int64Array&>(*arr).Value(i));
        default:
            throw std::runtime_error("Unsupported ts type: " + arr->type()->ToString());
    }
}

template <>
double ValueAt<double>(const std::shared_ptr<arrow::Array>& arr, int64_t i) {
    switch (arr->type_id()) {
        case arrow::Type::FLOAT:  return static_cast<double>(static_cast<const arrow::FloatArray&>(*arr).Value(i));
        case arrow::Type::DOUBLE: return static_cast<const arrow::DoubleArray&>(*arr).Value(i);
        default:
            throw std::runtime_error("Unsupported mid type: " + arr->type()->ToString());
    }
}

struct SpikeExample {
    uint32_t day;
    uint64_t ts_prev;
    uint64_t ts_curr;
    double   mid_prev;
    double   mid_curr;
    double   delta;
};

int main(int argc, char** argv) {
    std::string in_path, out_path;
    double threshold = 100.0;           // delete rows with |Δmid| >= threshold
    int64_t progress_every = 10'000'000;
    const double MID_MAX = 1000.0;      // delete rows with mid > MID_MAX
    const std::size_t MAX_EXAMPLES = 10;

    for (int i = 1; i < argc; ++i) {
        std::string a = argv[i];
        if (a == "--in" && i + 1 < argc) in_path = argv[++i];
        else if (a == "--out" && i + 1 < argc) out_path = argv[++i];
        else if (a == "--thr" && i + 1 < argc) threshold = std::stod(argv[++i]);
        else if (a == "--progress" && i + 1 < argc) progress_every = std::stoll(argv[++i]);
        else if (a == "--help" || a == "-h") usage_and_exit(argv[0]);
        else { std::fprintf(stderr, "Unknown or incomplete arg: %s\n", a.c_str()); usage_and_exit(argv[0]); }
    }
    if (in_path.empty() || out_path.empty()) usage_and_exit(argv[0]);

    try {
        std::filesystem::create_directories(std::filesystem::path(out_path).parent_path());
    } catch (...) {}

    // Input
    Result<std::shared_ptr<arrow::io::RandomAccessFile>> rf_res = arrow::io::ReadableFile::Open(in_path);
    if (!rf_res.ok()) { std::cerr << "open input failed: " << rf_res.status().ToString() << "\n"; return 1; }
    auto infile = *rf_res;

    auto fr_res = parquet::arrow::OpenFile(infile, arrow::default_memory_pool());
    if (!fr_res.ok()) { std::cerr << "open parquet reader failed: " << fr_res.status().ToString() << "\n"; return 1; }
    std::unique_ptr<parquet::arrow::FileReader> reader = std::move(fr_res).ValueOrDie();

    std::shared_ptr<arrow::Schema> schema;
    Status st = reader->GetSchema(&schema);
    if (!st.ok()) { std::cerr << "get schema failed: " << st.ToString() << "\n"; return 1; }

    auto ts_f  = schema->GetFieldByName("ts");
    auto mid_f = schema->GetFieldByName("mid");
    if (!ts_f || !mid_f) {
        std::cerr << "missing columns 'ts' and/or 'mid' in schema: " << schema->ToString() << "\n";
        return 1;
    }

    // Output
    auto of_res = arrow::io::FileOutputStream::Open(out_path);
    if (!of_res.ok()) { std::cerr << "open output failed: " << of_res.status().ToString() << "\n"; return 1; }
    auto outfile = *of_res;

    auto fw_res = parquet::arrow::FileWriter::Open(*schema, arrow::default_memory_pool(), outfile);
    if (!fw_res.ok()) { std::cerr << "create writer failed: " << fw_res.status().ToString() << "\n"; return 1; }
    std::unique_ptr<parquet::arrow::FileWriter> writer = std::move(fw_res).ValueOrDie();

    // Stream record batches over all row groups and all columns
    std::vector<int> all_row_groups;
    {
        auto* pq_reader = reader->parquet_reader();
        int nrg = pq_reader->metadata()->num_row_groups();
        all_row_groups.reserve(nrg);
        for (int i = 0; i < nrg; ++i) all_row_groups.push_back(i);
        std::cout << "=== " << in_path << " ===\n";
        std::cout << "  row_groups=" << nrg << " threshold=$" << threshold
                  << " mid_max=" << MID_MAX << "\n";
    }
    std::vector<int> all_cols(schema->num_fields());
    for (int i = 0; i < schema->num_fields(); ++i) all_cols[i] = i;

    std::shared_ptr<arrow::RecordBatchReader> rb_reader;
    st = reader->GetRecordBatchReader(all_row_groups, all_cols, &rb_reader);
    if (!st.ok()) { std::cerr << "GetRecordBatchReader failed: " << st.ToString() << "\n"; return 1; }

    uint64_t total_rows_in = 0, total_rows_out = 0, total_removed = 0;
    uint64_t removed_by_delta = 0, removed_by_level = 0;

    uint32_t last_day = 0;
    uint64_t last_ts = 0;
    double last_mid = 0.0;
    bool have_last = false;

    std::unordered_map<uint32_t, uint64_t> kept_per_day, removed_per_day;
    std::vector<SpikeExample> delta_examples;

    while (true) {
        std::shared_ptr<arrow::RecordBatch> batch;
        auto br = rb_reader->ReadNext(&batch);
        if (!br.ok()) { std::cerr << "ReadNext failed: " << br.ToString() << "\n"; return 1; }
        if (!batch) break;  // done

        const int64_t n = batch->num_rows();
        if (n == 0) continue;

        auto ts_arr  = batch->GetColumnByName("ts");
        auto mid_arr = batch->GetColumnByName("mid");
        if (!ts_arr || !mid_arr) { std::cerr << "batch missing 'ts' or 'mid'\n"; return 1; }

        arrow::BooleanBuilder keep_builder;
        keep_builder.Reserve(n);

        for (int64_t i = 0; i < n; ++i) {
            // null handling: drop null ts or null mid
            if (ts_arr->IsNull(i) || mid_arr->IsNull(i)) {
                keep_builder.UnsafeAppend(false);
                continue;
            }

            uint64_t ts = ValueAt<uint64_t>(ts_arr, i);
            double mid  = ValueAt<double>(mid_arr, i);

            uint32_t day = day_from_ts(ts);
            bool keep = true;
            bool big_delta = false;
            bool big_level = (mid > MID_MAX);

            if (!have_last || day != last_day) {
                // New day or no baseline yet: only apply level filter, no Δmid.
                if (big_level) {
                    keep = false;
                    removed_by_level++;
                    removed_per_day[day]++;
                    // do NOT update baseline; we want the next good tick to become first-of-day
                    have_last = false;
                } else {
                    keep = true;
                    kept_per_day[day]++;
                    last_day = day;
                    last_mid = mid;
                    last_ts  = ts;
                    have_last = true;
                }
            } else {
                // Same day, we can compute Δmid vs last kept mid.
                double delta = std::fabs(mid - last_mid);
                big_delta = (delta >= threshold);

                if (big_delta) {
                    keep = false;
                    removed_by_delta++;
                    removed_per_day[day]++;
                    if (delta_examples.size() < MAX_EXAMPLES) {
                        SpikeExample ex;
                        ex.day       = day;
                        ex.ts_prev   = last_ts;
                        ex.ts_curr   = ts;
                        ex.mid_prev  = last_mid;
                        ex.mid_curr  = mid;
                        ex.delta     = delta;
                        delta_examples.push_back(ex);
                    }
                    // do NOT update baseline: last_mid/last_ts stay as last kept tick
                } else if (big_level) {
                    keep = false;
                    removed_by_level++;
                    removed_per_day[day]++;
                    // also do NOT update baseline
                } else {
                    keep = true;
                    kept_per_day[day]++;
                    last_mid = mid;
                    last_ts  = ts;
                    last_day = day;
                }
            }

            if (!(!have_last || day != last_day) && keep == false) {
                // already accounted into removed_per_day in the branches above
            } else if (!have_last || day != last_day) {
                // removed_per_day updated above for big_level; kept_per_day too
            }

            keep_builder.UnsafeAppend(keep);
            total_rows_in++;
            if (!keep) total_removed++;
        }

        std::shared_ptr<arrow::Array> keep_mask;
        st = keep_builder.Finish(&keep_mask);
        if (!st.ok()) { std::cerr << "mask finish failed: " << st.ToString() << "\n"; return 1; }

        arrow::compute::FilterOptions fopts(arrow::compute::FilterOptions::DROP);
        auto f_res = arrow::compute::Filter(arrow::Datum(batch), arrow::Datum(keep_mask), fopts);
        if (!f_res.ok()) { std::cerr << "Filter failed: " << f_res.status().ToString() << "\n"; return 1; }
        auto out_batch = f_res->record_batch();
        if (!out_batch) {
            // if everything dropped, record_batch() can be null-ish; build an empty batch with same schema
            out_batch = arrow::RecordBatch::Make(batch->schema(), /*num_rows=*/0, batch->columns());
        }

        total_rows_out += static_cast<uint64_t>(out_batch->num_rows());

        if (out_batch->num_rows() > 0) {
            auto wst = writer->WriteRecordBatch(*out_batch);
            if (!wst.ok()) { std::cerr << "WriteRecordBatch failed: " << wst.ToString() << "\n"; return 1; }
        }

        if (progress_every > 0 && (total_rows_in % progress_every) < static_cast<uint64_t>(out_batch->num_rows())) {
            std::cout << "    processed rows: " << total_rows_in
                      << " kept: " << total_rows_out
                      << " removed: " << total_removed << "\n";
        }
    }

    // Close writer/stream
    auto cls = writer->Close();
    if (!cls.ok()) { std::cerr << "writer close failed: " << cls.ToString() << "\n"; return 1; }
    auto ofs = outfile->Close();
    if (!ofs.ok()) { std::cerr << "outfile close failed: " << ofs.ToString() << "\n"; return 1; }

    // Print per-day removals
    std::vector<uint32_t> days;
    days.reserve(kept_per_day.size() + removed_per_day.size());
    for (auto& kv : kept_per_day)   days.push_back(kv.first);
    for (auto& kv : removed_per_day) days.push_back(kv.first);
    std::sort(days.begin(), days.end());
    days.erase(std::unique(days.begin(), days.end()), days.end());

    std::cout << "  per-day removed counts:\n";
    for (auto d : days) {
        uint64_t kept = kept_per_day.count(d) ? kept_per_day[d] : 0;
        uint64_t removed = removed_per_day.count(d) ? removed_per_day[d] : 0;
        if (removed > 0) {
            std::cout << "    " << day_to_string(d)
                      << " removed=" << removed
                      << " kept=" << kept << "\n";
        }
    }

    // Print some examples of big Δmid spikes
    std::cout << "  sample big-Δmid pairs (|Δmid| >= " << threshold << "):\n";
    if (delta_examples.empty()) {
        std::cout << "    none\n";
    } else {
        for (const auto& ex : delta_examples) {
            std::cout << "    day=" << day_to_string(ex.day)
                      << " ts_prev=" << ex.ts_prev
                      << " ts_curr=" << ex.ts_curr
                      << " mid_prev=" << ex.mid_prev
                      << " mid_curr=" << ex.mid_curr
                      << " |Δmid|=" << ex.delta
                      << "\n";
        }
    }

    std::cout << "=== summary ===\n";
    std::cout << "  in_rows=" << total_rows_in
              << " out_rows=" << total_rows_out
              << " removed=" << total_removed
              << " kept_ratio=" << (total_rows_in ? (double)total_rows_out / (double)total_rows_in : 1.0)
              << "\n";
    std::cout << "  removed_by_delta=" << removed_by_delta
              << " removed_by_level=" << removed_by_level
              << "\n";

    return 0;
}
