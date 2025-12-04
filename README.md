# hft-lab

General overview about the repo here...to be completed later

# Install Instructions

### 1. Install Apache Arrow (Homebrew / macOS Silicon)

```bash
brew update
brew install apache-arrow
brew install nlohmann-json
```

### 2. Configure and build

```bash
chmod +x ./nbbo_pipeline/scripts/*
./nbbo_pipeline/scripts/build.sh
```

### 3. Run Data Processing Pipeline

**TODO**
Instructions here for running pipeline to get `data/out/even_clean/SPY_YYYY.parquet`.
Should entail full process from ingesting the `csv.gz` file to get the `event_clean` files

### 4. Run Build Events Pipeline

```bash
./nbbo_pipeline/scripts/run_build_events.sh
```

Output: `data/research/events/SPY_YYYY_events.parquet`

### 5. Run Build Histogram Pipeline

```bash
./nbbo_pipeline/scripts/run_build_histogram.sh
```

### 6. Run Backtester (Workflow C)

```bash
./nbbo_pipeline/scripts/run_backtester.sh
```

Output: `data/research/hist/SPY_histogram.json`
