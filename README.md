# HFT-Lab Instruction Manual

## Project Overview

HFT-Lab is a C++ research codebase for building and testing high-frequency trading strategies on real SPY Level-1 quote data. Starting from raw exchange `.csv.gz` files, the pipeline constructs a cleaned National Best Bid and Offer (NBBO), resamples it onto different 1 ms grids, removes data glitches/anomalies, generates features to build trades, and then feeds the result into a backtesting framework. The goal is not to implement a production trading system, but to provide a transparent, reproducible lab where one can study how microstructure features (spread, volume imbalance, queue ages, last price move, etc.) relate to short-horizon price moves and how a basic strategy would have performed.

# Install Instructions

## 1. Install via Homebrew on macOS Silicon

```bash
brew update
brew install apache-arrow
brew install nlohmann-json
```

**Note**: this project was only tested on macOS Silicon machines. Some build/run commands may not work if you are not on macOS.

# Run Instructions

## 1. Configure and build

```bash
chmod +x ./nbbo_pipeline/scripts/*.sh
./nbbo_pipeline/scripts/build.sh
```

## 2. Run Data Processing Pipeline

The `nbbo_pipeline` is the first stage of the project and converts raw exchange quote files (`SPY{YYYY}.csv.gz`) into clean, structured NBBO datasets. These datasets form the foundation for all later workflows: denoising, event labeling, histogram modeling, and backtesting.

At a high level, the pipeline loads every quote, computes the NBBO at each millisecond, filters out invalid or out-of-hours data, handles missing data via forward-fill (when requested), optionally winsorizes extreme returns, and writes partitioned Parquet files by year.

The pipeline can generate four different "views" of the mid-price stream. A mid-price is defined as:

```
mid_price = (max_bid_price + min_ask_price) / 2
```

Different output options:

- **Event grid** (`data/out/event/`) – emits a row only when the mid-price changes.
- **Event-winsor grid** (`data/out/event_winsor/`) – same as Event grid, but extreme log returns are clipped (default: 0.00001 / 0.99999 quantiles).
- **Clock grid** (`data/out/clock/`) – emits a row every millisecond, forward-filling missing NBBO values for gaps `<= 250 ms`.
- **Clock-winsor grid** (`data/out/clock_winsor/`) – same as Clock grid, with winsorized returns.

**IMPORTANT NOTE:** our project primarily uses the **event** grids because later workflows operate specifically on mid-price change events. The rest of the documentation will reference clean event data sourced from `data/out/event/` (non-winsorized) as its input directory.

### How the Data Processing Pipeline Works

`nbbo_pipeline/src/nbbo_pipeline.cpp` is the core file. In summary:

**Stage A: CSV -> msbin**

- Reads each `csv.gz` file
- Filters quotes to regular trading hours
- Computes the best bid/ask for each millisecond
- Computes mid-prices and log returns
- Writes a binary `.msbin` stream into a cache.

**Stage B: Tail quantile estimation (optional)**

- If winsorization is enabled, the pipeline scans all `log_return` values in parallel and computes extreme quantiles (e.g. 0.00001 / 0.99999).

**Stage C: Parquet writer**

- Streams `.msbin` files
- Applies winsor clipping or dropping
- Partitions rows by year
- Writes final Parquet files under the appropriate mode directory (`event/`, `event_winsor/`, etc.).

**Stage D: Reporting**

- All detected data issues (locked/crossed quotes, non-positive sizes, parse failures, etc.) are summarized in a human-readable glitch report.

### Scripts for Running Each Case

Each shell script is a wrapper around `nbbo_pipeline`, setting the correct flags for event/clock modes and winsorization. See `nbbo_pipeline/scripts/` for dedicated run scripts.

**Input**: `data/in/SPY{YYYY}.csv.gz`

---

**Run command:**

Produces the raw event grid, writing one row per mid-price change.

```bash
./nbbo_pipeline/scripts/run_event.sh
```

**Output:** `data/out/event/SPY_{YYYY}.parquet`

---

**Run command:**

Produces the winsorized event grid, clipping extreme log returns.

```bash
./nbbo_pipeline/scripts/run_event_winsor.sh
```

**Output:** `data/out/event_winsor/SPY_{YYYY}.parquet`

---

**Run command:**

Produces a 1 ms clock grid.

```bash
nbbo_pipeline/scripts/run_clock.sh
```

**Output:** `data/out/clock/SPY_{YYYY}.parquet`

---

**Run command:**

Produces a winsorized 1 ms clock grid.

```bash
nbbo_pipeline/scripts/run_clock_winsor.sh
```

**Output:** `data/out/clock_winsor/SPY_{YYYY}.parquet`

---

## 3. Run Clean Mid Spikes Pipeline

The clean mid spikes pipeline takes the event-grid mid-price series from `data/out/event/` and removes undesirable ticks before generating features. It scans each day, drops rows where the mid-price itself is above a threshold (default: `mid > $1000`) or where the mid-price makes a single-tick jump larger than a threshold (default: `|Δmid| >= $100`), measured relative to the last kept tick in that day. The result is a denoised event grid written to `data/out/event_clean/` that preserves realistic moves while removing outliers.

**Note**: this example only references the output from `run_event.sh` in the **output**.

**Run command:**

```bash
./nbbo_pipeline/scripts/run_denoise.sh
```

**Input:**

```bash
data/out/event/SPY_{YYYY}.parquet
```

**Output:**

```bash
data/out/event_clean/SPY_{YYYY}.parquet
```

## 4. Run Build Events Pipeline

The Build Events pipeline takes the cleaned event-grid NBBO data (`data/out/event_clean/`) and converts it into mid-price change events, each enriched with features and labeled with the next mid-price move on the same trading day. This transforms the raw tick-level mid-price stream into a dataset where each row describes a mid-price change, its context (spread, imbalance, quote ages, last move sign), and the next observed price movement and waiting time. The output is stored as Parquet files under `data/research/events/`.

Each event is output only when the mid-price changes (i.e., `log_return != 0`) and includes features such as imbalance, bid/ask quote ages, spread, and the last-move direction, while the label is computed by looking ahead to the next mid-change within the same day. Very large mid jumps (`|mid_next − mid| > threshold_next`) are treated as outliers and dropped.

**Run command:**

```bash
./nbbo_pipeline/scripts/run_build_events.sh
```

**Input:**

```bash
data/out/event_clean/SPY_{YYYY}.parquet
```

**Output:**

```bash
data/research/events/SPY_{YYYY}.parquet
```

## 5. Run Build Histogram Pipeline

The histogram pipeline aggregates all labeled mid-change events into a 4-dimensional discretized model that estimates the probability of an uptick, the probability of a downtick, the direction score, and the expected waiting time until the next mid-price change. The state space consists of four discrete bins:

- Imbalance bin
- Spread bin
- Age-difference bin (age of bid – age of ask)
- Last-move bin (−1, 0, +1)

For each state cell, the histogram accumulates:

- `n`: number of observations in the cell
- `n_up`: number of events with Y = +1
- `n_down`: number of events with Y = -1
- `sum_tau_ms`: sum of waiting times for events in the cell

After processing all events across all years, the pipeline writes a single JSON file that encodes the bins, per-cell statistics, smoothed probabilities, direction scores, and mean waiting times. This JSON is the input to the backtester.

A default config file is provided, but one can edit the histogram bins as needed within the `nbbo_pipeline/config/hist_bins_default.json` file as needed for additional customization.

**Run command:**

```bash
./nbbo_pipeline/scripts/run_build_histogram.sh
```

**Input:**

```bash
data/research/events/SPY_{YYYY}.parquet
```

**Output:**

```bash
data/research/hist/SPY_histogram.json
```

## 6. Run Backtester

The backtester consumes the labeled events `data/research/events/` and the histogram model (`data/research/hist/SPY_histogram.json`) to simulate a state-based trading strategy. For each mid-change event, the backtester uses the histogram to calculate direction score, expected edge, and waiting-time constraints. Trades are opened when the strategy criteria are satisfied, and PnL is aggregated at both the trade level and daily level. The pipeline writes final CSVs: per-trade logs and per-day PnL summaries.

**Run command:**

```bash
./nbbo_pipeline/scripts/run_backtester.sh
```

**Input:**

```bash
data/research/events/SPY_{YYYY}.parquet
data/research/hist/SPY_histogram.json
```

**Output:**

```bash
data/research/trades/SPY_{YYYY}_trades.csv
data/research/daily/SPY_{YYYY}_daily.csv
```

## 7. Run Summarize Trades

The summarize trades tool computes year-by-year performance statistics from the backtester’s per-trade CSVs in `data/research/trades/`. For each year, it aggregates total net return, the number of trades, win/loss counts, average win/loss, and best/worst trades. The output is a human-readable text report that can be inspected directly or tracked across different strategy configurations.

By default, the helper script summarizes the years 2018–2023, but you can pass a custom list or range of years on the command line (e.g., `2019-2021`, `2018 2020 2022`, or `2018 2020-2022 2024`).

**Run command:**

```bash
./nbbo_pipeline/scripts/summarize_trades.sh
```

**Input:**

```bash
data/research/trades/SPY_{YYYY}_trades.csv
```

To use a custom year range or list, pass it as arguments to the script:

```bash
# Single range
./nbbo_pipeline/scripts/summarize_trades.sh 2019-2021

# Mixed list + range
./nbbo_pipeline/scripts/summarize_trades.sh 2018 2020-2022 2024
```

**Output:**
```bash
data/research/summary/yearly_pnl.txt
```

