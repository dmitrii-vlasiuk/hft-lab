#pragma once

#include <chrono>
#include <mutex>
#include <string>
#include <thread>
#include <vector>

namespace nbbo {

struct TimingEntry {
  std::string name;
  std::chrono::steady_clock::duration duration;
};

class TimingRegistry {
 public:
  static TimingRegistry& Instance();

  void Add(std::string name, std::chrono::steady_clock::duration d);

  const std::vector<TimingEntry>& Entries() const { return entries_; }

 private:
  TimingRegistry() = default;

  mutable std::mutex mu_;
  std::vector<TimingEntry> entries_;
};

class ScopeTimer {
 public:
  explicit ScopeTimer(std::string name)
      : name_(std::move(name)),
        start_(std::chrono::steady_clock::now()) {}

  ~ScopeTimer() {
    const auto end = std::chrono::steady_clock::now();
    TimingRegistry::Instance().Add(name_, end - start_);
  }

 private:
  std::string name_;
  std::chrono::steady_clock::time_point start_;
};

/// Helper macro so you can write: NBBO_SCOPE_TIMER("step_name");
#define NBBO_SCOPE_TIMER(label) \
  ::nbbo::ScopeTimer nbbo_scope_timer_##__LINE__(label)

/// Append a timing report for the current run to a log file.
///
/// `append` defaults to true so multiple runs across multiple binaries can
/// share a single timing_log.txt.
void WriteTimingReport(const std::string& out_path,
                       const std::string& program_name,
                       const std::vector<std::string>& args,
                       bool append = true);

}  // namespace nbbo
