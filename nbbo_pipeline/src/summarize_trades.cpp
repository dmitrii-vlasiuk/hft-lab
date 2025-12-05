#include <algorithm>
#include <cstdint>
#include <cstdlib>
#include <fstream>
#include <iomanip>
#include <iostream>
#include <sstream>
#include <string>
#include <vector>

// Simple helper to expand years from CLI arguments.
//
// Supports two formats:
//   - Individual years: "2018 2019 2020"
//   - Ranges:           "2018-2023"
// You can also mix them, e.g. "2018-2020 2022".
static std::vector<int> expand_years(int argc, char** argv, int start_index) {
  std::vector<int> years;
  for (int i = start_index; i < argc; ++i) {
    std::string token = argv[i];
    auto dash_pos = token.find('-');
    if (dash_pos != std::string::npos) {
      // Parse "YYYY-YYYY" as an inclusive range
      int y1 = std::atoi(token.substr(0, dash_pos).c_str());
      int y2 = std::atoi(token.substr(dash_pos + 1).c_str());
      if (y2 < y1) {
        std::cerr << "Invalid year range: " << token << "\n";
        std::exit(1);
      }
      for (int y = y1; y <= y2; ++y) {
        years.push_back(y);
      }
    } else {
      // Single year, e.g. "2019"
      int y = std::atoi(token.c_str());
      years.push_back(y);
    }
  }
  // Sort and dedupe in case of overlaps/duplicates
  std::sort(years.begin(), years.end());
  years.erase(std::unique(years.begin(), years.end()), years.end());
  return years;
}

// Given a CSV line from SPY_YYYY_trades.csv, extract the net_ret field.
//
// Expected schema:
//   ts_in,ts_out,day,mid_in,mid_out,spread_in,
//   direction_score,expected_edge_ret,cost_ret,gross_ret,net_ret,side
//
// We want the 11th field (0-based index 10).
// Implementation: count commas until we find the slice for net_ret.
static bool extract_net_ret(const std::string& line, double& out_net_ret) {
  std::size_t comma_count = 0;
  std::size_t start = std::string::npos;
  std::size_t end = std::string::npos;

  for (std::size_t i = 0; i < line.size(); ++i) {
    if (line[i] == ',') {
      ++comma_count;
      if (comma_count == 10) {
        // net_ret starts after the 10th comma
        start = i + 1;
      } else if (comma_count == 11) {
        // net_ret ends at the 11th comma
        end = i;
        break;
      }
    }
  }

  if (start == std::string::npos) {
    // Not enough columns
    return false;
  }
  if (end == std::string::npos) {
    // net_ret is the last field; use until end-of-line
    end = line.size();
  }
  if (end <= start) {
    return false;
  }

  std::string field = line.substr(start, end - start);
  try {
    out_net_ret = std::stod(field);
  } catch (...) {
    // Malformed number; skip this line
    return false;
  }
  return true;
}

// Per-year summary aggregates computed from SPY_<year>_trades.csv.
struct YearStats {
  double total_net_ret = 0.0;      // Sum of net_ret across all trades
  std::uint64_t num_trades = 0;    // Total number of trades
  std::uint64_t num_wins = 0;      // Trades with net_ret > 0
  std::uint64_t num_losses = 0;    // Trades with net_ret < 0
  std::uint64_t num_flat = 0;      // Trades with net_ret == 0

  double sum_win_net = 0.0;        // Sum of net_ret for winning trades
  double sum_loss_net = 0.0;       // Sum of net_ret for losing trades (negative)

  double max_gain = -1e300;        // Best trade (largest net_ret)
  double max_loss =  1e300;        // Worst trade (smallest net_ret)
};

// Read one year's trades CSV and accumulate YearStats.
//
// Expects file at: <trades_dir>/SPY_<year>_trades.csv
// Skips malformed lines; exits if file can't be opened.
static YearStats summarize_year(const std::string& trades_dir, int year) {
  YearStats stats;
  std::ostringstream fname;
  fname << "SPY_" << year << "_trades.csv";

  // Build "<trades_dir>/SPY_<year>_trades.csv" with a simple slash check
  std::string path = trades_dir;
  if (!path.empty() && path.back() != '/' && path.back() != '\\') {
    path += '/';
  }
  path += fname.str();

  std::ifstream in(path);
  if (!in) {
    std::cerr << "Failed to open trades file for " << year << ": " << path << "\n";
    std::exit(1);
  }

  std::string line;

  // Read and discard header
  if (!std::getline(in, line)) {
    std::cerr << "Empty trades file for " << year << ": " << path << "\n";
    return stats;
  }

  // Process each trade line
  while (std::getline(in, line)) {
    if (line.empty()) {
      continue;
    }

    double net_ret = 0.0;
    if (!extract_net_ret(line, net_ret)) {
      // Poorly-formed line; skip
      continue;
    }

    stats.num_trades++;
    stats.total_net_ret += net_ret;

    if (net_ret > 0.0) {
      stats.num_wins++;
      stats.sum_win_net += net_ret;
      stats.max_gain = std::max(stats.max_gain, net_ret);
    } else if (net_ret < 0.0) {
      stats.num_losses++;
      stats.sum_loss_net += net_ret;
      stats.max_loss = std::min(stats.max_loss, net_ret);
    } else {
      stats.num_flat++;
    }
  }

  return stats;
}

int main(int argc, char** argv) {
  // CLI:
  //   summarize_yearly_pnl <trades_dir> <years...>
  //
  // Examples:
  //   summarize_yearly_pnl data/research/trades 2018-2023
  //   summarize_yearly_pnl data/research/trades 2018 2019 2020
  if (argc < 3) {
    std::cerr << "Usage: " << argv[0]
              << " <trades_dir> <years...>\n\n"
              << "Examples:\n"
              << "  " << argv[0]
              << " data/research/trades 2018-2023\n"
              << "  " << argv[0]
              << " data/research/trades 2018 2019 2020\n";
    return 1;
  }

  std::string trades_dir = argv[1];
  // Parse all remaining args into a sorted, deduped list of years
  std::vector<int> years = expand_years(argc, argv, 2);

  std::cout << "Using trades directory: " << trades_dir << "\n";
  std::cout << "Years: ";
  for (std::size_t i = 0; i < years.size(); ++i) {
    if (i) std::cout << ", ";
    std::cout << years[i];
  }
  std::cout << "\n\n";

  // Global formatting: default to 8 decimal places for returns
  std::cout << std::fixed << std::setprecision(8);

  std::string header =
      "  Year   Total Net Ret   Total Net Ret (bps)    # Trades   Win%   Loss%  "
      "Avg Win    Avg Loss     Max Gain     Max Loss";
  std::cout << header << "\n";
  std::cout << std::string(header.size(), '-') << "\n";

  for (int y : years) {
    YearStats stats = summarize_year(trades_dir, y);

    double total_net = stats.total_net_ret;
    // Returns are in units of "fraction"; multiply by 1e4 to get basis points.
    double total_net_bps = total_net * 1e4;

    std::uint64_t n = stats.num_trades;
    double win_pct  = (n > 0)
                          ? 100.0 * static_cast<double>(stats.num_wins) / n
                          : 0.0;
    double loss_pct = (n > 0)
                          ? 100.0 * static_cast<double>(stats.num_losses) / n
                          : 0.0;

    double avg_win  = (stats.num_wins > 0)
                          ? stats.sum_win_net /
                                static_cast<double>(stats.num_wins)
                          : 0.0;
    double avg_loss = (stats.num_losses > 0)
                          ? stats.sum_loss_net /
                                static_cast<double>(stats.num_losses)
                          : 0.0;  // negative

    double max_gain = (stats.num_wins > 0) ? stats.max_gain : 0.0;
    double max_loss = (stats.num_losses > 0) ? stats.max_loss : 0.0;

    // Note: we temporarily change precision to pretty-print each column,
    // then reset it back to 8 at the end of the line.
    std::cout << std::setw(6) << y << "  "
              << std::setw(15) << total_net << "  "
              << std::setw(20) << total_net_bps << "  "
              << std::setw(10) << stats.num_trades << "  "
              << std::setw(6) << std::setprecision(2) << win_pct << "  "
              << std::setw(6) << std::setprecision(2) << loss_pct << "  "
              << std::setw(8) << std::setprecision(6) << avg_win << "  "
              << std::setw(10) << std::setprecision(6) << avg_loss << "  "
              << std::setw(10) << std::setprecision(6) << max_gain << "  "
              << std::setw(10) << std::setprecision(6) << max_loss
              << std::setprecision(8)  // reset precision for the next row
              << "\n";
  }

  return 0;
}
