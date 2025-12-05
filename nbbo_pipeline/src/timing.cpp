#include "nbbo/timing.hpp"

#include <chrono>
#include <ctime>
#include <filesystem>
#include <fstream>
#include <iomanip>
#include <iostream>

namespace nbbo {

TimingRegistry& TimingRegistry::Instance() {
  static TimingRegistry instance;
  return instance;
}

void TimingRegistry::Add(std::string name,
                         std::chrono::steady_clock::duration d) {
  std::lock_guard<std::mutex> lock(mu_);
  entries_.push_back(TimingEntry{std::move(name), d});
}

namespace {

double DurationMillis(const std::chrono::steady_clock::duration& d) {
  using ms = std::chrono::duration<double, std::milli>;
  return std::chrono::duration_cast<ms>(d).count();
}

}  // namespace

void WriteTimingReport(const std::string& out_path,
                       const std::string& program_name,
                       const std::vector<std::string>& args,
                       bool append) {
  // Ensure parent directory exists.
  std::filesystem::path p(out_path);
  if (p.has_parent_path()) {
    std::error_code ec;
    std::filesystem::create_directories(p.parent_path(), ec);
    if (ec) {
      std::cerr << "Warning: failed to create timing directory: "
                << ec.message() << "\n";
    }
  }

  std::ios_base::openmode mode = std::ios::out;
  if (append) mode |= std::ios::app;

  std::ofstream out(out_path, mode);
  if (!out) {
    std::cerr << "Failed to open timing report file: " << out_path << "\n";
    return;
  }

  // Separator between runs.
  out << "\n";
  out << "============================================================\n";

  // Timestamp (system clock).
  auto now = std::chrono::system_clock::now();
  std::time_t t = std::chrono::system_clock::to_time_t(now);
  out << "timestamp: " << std::put_time(std::localtime(&t), "%F %T") << "\n";

  // Basic “system” info.
  out << "program: " << program_name << "\n";
  out << "args:";
  for (const auto& a : args) {
    out << " " << a;
  }
  out << "\n";

  const unsigned hw_threads = std::thread::hardware_concurrency();
  out << "hardware_concurrency: "
      << (hw_threads == 0 ? 1 : hw_threads) << "\n\n";

  // Table header.
  out << std::left << std::setw(40) << "step"
      << std::right << std::setw(15) << "ms"
      << std::right << std::setw(15) << "seconds"
      << "\n";

  out << std::string(70, '-') << "\n";

  const auto& entries = TimingRegistry::Instance().Entries();
  for (const auto& e : entries) {
    const double ms = DurationMillis(e.duration);
    out << std::left << std::setw(40) << e.name
        << std::right << std::setw(15) << std::fixed << std::setprecision(3)
        << ms
        << std::right << std::setw(15) << std::fixed << std::setprecision(3)
        << (ms / 1000.0)
        << "\n";
  }
}

}  // namespace nbbo
