// nbbo_pipeline/src/strategy_config.cpp
#include "nbbo/backtester.hpp"

#include <fstream>
#include <stdexcept>
#include <string>

#include <nlohmann/json.hpp>

namespace nbbo {

using nlohmann::json;

// from_json adapter so StrategyConfig can be constructed via j.get<StrategyConfig>().
//
// This keeps the existing field names and defaults from StrategyConfig in
// backtester.hpp, but uses a real JSON library instead of a hand-rolled parser.
// It also supports a couple of backwards-compat alias keys.
void from_json(const json& j, StrategyConfig& cfg) {
  // Start from the struct defaults declared in backtester.hpp.
  cfg = StrategyConfig{};

  if (j.contains("fee_price")) {
    j.at("fee_price").get_to(cfg.fee_price);
  }
  if (j.contains("slip_price")) {
    j.at("slip_price").get_to(cfg.slip_price);
  }
  if (j.contains("min_abs_direction_score")) {
    j.at("min_abs_direction_score").get_to(cfg.min_abs_direction_score);
  }
  if (j.contains("min_expected_edge_bps")) {
    j.at("min_expected_edge_bps").get_to(cfg.min_expected_edge_bps);
  }
  if (j.contains("max_mean_wait_ms")) {
    j.at("max_mean_wait_ms").get_to(cfg.max_mean_wait_ms);
  }

  // Primary edge_mode selector (0 = legacy, 1 = Mode A, 2 = Mode B).
  int mode_int = 2;  // default to "CostWithGate"
  if (j.contains("edge_mode")) {
    mode_int = j.at("edge_mode").get<int>();
  }
  cfg.edge_mode = static_cast<EdgeMode>(mode_int);

  // Backwards-compat alias: legacy_mode != 0 forces edge_mode = Legacy.
  int legacy_mode = 0;
  if (j.contains("legacy_mode")) {
    legacy_mode = j.at("legacy_mode").get<int>();
  }
  if (legacy_mode != 0) {
    cfg.edge_mode = EdgeMode::Legacy;
  }

  // Optional backwards-compat aliases with different naming.
  // If present and the primary key is absent, use the alias.
  if (j.contains("fee_per_leg") && !j.contains("fee_price")) {
    j.at("fee_per_leg").get_to(cfg.fee_price);
  }
  if (j.contains("min_expected_edge") && !j.contains("min_expected_edge_bps")) {
    j.at("min_expected_edge").get_to(cfg.min_expected_edge_bps);
  }
}

// Load StrategyConfig from a JSON file.
StrategyConfig LoadStrategyConfig(const std::string& path) {
  std::ifstream in(path);
  if (!in) {
    throw std::runtime_error("Failed to open strategy config: " + path);
  }

  json j;
  in >> j;
  return j.get<StrategyConfig>();
}

}  // namespace nbbo
