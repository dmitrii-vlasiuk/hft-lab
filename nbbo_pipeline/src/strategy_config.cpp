// nbbo_pipeline/src/strategy_config.cpp
#include "nbbo/backtester.hpp"

#include <cctype>
#include <fstream>
#include <sstream>
#include <stdexcept>
#include <string>

namespace {

// Very lightweight JSON number extractor for flat config keys.
// This is NOT a full JSON parser—just enough to grab
//   "<key>": <number>
// assuming flat config files.
double ExtractDouble(const std::string& json,
                     const std::string& key,
                     double default_value) {
  const std::string quoted_key = "\"" + key + "\"";
  auto pos = json.find(quoted_key);
  if (pos == std::string::npos) {
    return default_value;
  }
  pos = json.find(':', pos);
  if (pos == std::string::npos) {
    return default_value;
  }
  ++pos;
  while (pos < json.size() &&
         std::isspace(static_cast<unsigned char>(json[pos]))) {
    ++pos;
  }

  std::string number_str;
  while (pos < json.size()) {
    char c = json[pos];
    if ((c >= '0' && c <= '9') || c == '-' || c == '+' ||
        c == '.' || c == 'e' || c == 'E') {
      number_str.push_back(c);
      ++pos;
    } else {
      break;
    }
  }

  if (number_str.empty()) {
    return default_value;
  }

  try {
    return std::stod(number_str);
  } catch (...) {
    return default_value;
  }
}

}  // namespace

namespace nbbo {

// Loads the trading strategy configuration from a flat JSON file.
// Fields it expects (with defaults from StrategyConfig in backtester.hpp):
//   - fee_price              : per-leg fee in *price* units (e.g. $0.03/share)
//   - slip_price             : extra slippage cushion in price units
//   - min_abs_direction_score: minimum |signal| to trade
//   - min_expected_edge_bps  : optional edge gate in bps of return
//   - max_mean_wait_ms       : optional cap on expected waiting time (from histogram)
//   - edge_mode              : 0 = legacy, 1 = new Mode A, 2 = new Mode B
//   - legacy_mode            : backwards-compat alias; nonzero => edge_mode = 0
StrategyConfig LoadStrategyConfig(const std::string& path) {
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("Failed to open strategy config: " + path);
  }
  std::ostringstream oss;
  oss << in.rdbuf();
  const std::string json = oss.str();

  StrategyConfig cfg;
  cfg.fee_price =
      ExtractDouble(json, "fee_price", cfg.fee_price);
  cfg.slip_price =
      ExtractDouble(json, "slip_price", cfg.slip_price);
  cfg.min_abs_direction_score =
      ExtractDouble(json, "min_abs_direction_score",
                    cfg.min_abs_direction_score);
  cfg.min_expected_edge_bps =
      ExtractDouble(json, "min_expected_edge_bps",
                    cfg.min_expected_edge_bps);
  cfg.max_mean_wait_ms =
      ExtractDouble(json, "max_mean_wait_ms",
                    cfg.max_mean_wait_ms);

  // New: edge_mode (0 = legacy, 1 = new Mode A, 2 = new Mode B)
  const double edge_mode_val =
      ExtractDouble(json, "edge_mode",
                    static_cast<double>(cfg.edge_mode));
  cfg.edge_mode = static_cast<int>(edge_mode_val);

  // Backwards-compat alias: legacy_mode != 0 forces edge_mode = 0
  const double legacy_val = ExtractDouble(json, "legacy_mode", 0.0);
  if (legacy_val != 0.0) {
    cfg.edge_mode = 0;
  }

  return cfg;
}

}  // namespace nbbo
