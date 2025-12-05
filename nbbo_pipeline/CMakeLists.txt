cmake_minimum_required(VERSION 3.20)
project(nbbo_pipeline CXX)

# Basics
set(CMAKE_CXX_STANDARD 23)
set(CMAKE_CXX_STANDARD_REQUIRED ON)
set(CMAKE_POSITION_INDEPENDENT_CODE ON)
if (NOT CMAKE_BUILD_TYPE)
  set(CMAKE_BUILD_TYPE Release)
endif()

# Deps
find_package(Arrow REQUIRED)
find_package(Parquet REQUIRED)
find_package(ZLIB REQUIRED)
find_package(Threads REQUIRED)

# Normalize Arrow/Parquet target names
if (TARGET Arrow::arrow_shared)
  set(ARROW_TGT Arrow::arrow_shared)
elseif (TARGET Arrow::arrow)
  set(ARROW_TGT Arrow::arrow)
else()
  message(FATAL_ERROR "Could not find an Arrow imported target")
endif()

if (TARGET Parquet::parquet_shared)
  set(PARQUET_TGT Parquet::parquet_shared)
elseif (TARGET Parquet::parquet)
  set(PARQUET_TGT Parquet::parquet)
else()
  message(FATAL_ERROR "Could not find a Parquet imported target")
endif()

# ------------------------------------------------------------------------------
# Core header-only library with shared helpers
# ------------------------------------------------------------------------------
add_library(nbbo_core INTERFACE)

target_include_directories(nbbo_core
  INTERFACE
    ${CMAKE_CURRENT_SOURCE_DIR}/include
)

target_link_libraries(nbbo_core
  INTERFACE
    ${ARROW_TGT}
    ${PARQUET_TGT}
)

target_compile_features(nbbo_core INTERFACE cxx_std_23)

# ----------------------------------------------------------------------
# Timing library
# ----------------------------------------------------------------------
add_library(nbbo_timing
  src/timing.cpp
)

target_link_libraries(nbbo_timing
  PUBLIC
    nbbo_core
)

target_compile_features(nbbo_timing PUBLIC cxx_std_23)

# ------------------------------------------------------------------------------
# Helper to define nbbo tools
# ------------------------------------------------------------------------------
function(add_nbbo_tool name)
  # Remaining args are sources
  add_executable(${name} ${ARGN})

  target_link_libraries(${name}
    PRIVATE
      nbbo_core
      nbbo_timing
      ZLIB::ZLIB
      Threads::Threads
  )
  target_compile_features(${name} PRIVATE cxx_std_23)
  target_compile_options(${name} PRIVATE -O3 -pipe -fvisibility=hidden)
endfunction()

# ------------------------------------------------------------------------------
# Command-line tools
# ------------------------------------------------------------------------------

# nbbo_pipeline
add_nbbo_tool(nbbo_pipeline src/nbbo_pipeline.cpp)

# clean_mid_spikes
add_nbbo_tool(clean_mid_spikes src/clean_mid_spikes.cpp)

# build_events
add_nbbo_tool(build_events
  src/build_events.cpp
  src/event_table_builder.cpp
)

# ----------------------------------------------------------------------
# Histogram library (model + bins)
# ----------------------------------------------------------------------
add_library(nbbo_histogram
  src/histogram_model.cpp
  src/histogram_bins.cpp
)

# Inherit include dirs + Arrow/Parquet from nbbo_core
target_link_libraries(nbbo_histogram
  PUBLIC
    nbbo_core
)

# ----------------------------------------------------------------------
# build_histogram tool
# ----------------------------------------------------------------------
add_nbbo_tool(build_histogram
  src/build_histogram.cpp
  src/histogram_builder.cpp
)

# Link against nbbo_histogram (which already pulls in nbbo_core)
target_link_libraries(build_histogram
  PRIVATE
    nbbo_histogram
)

# ----------------------------------------------------------------------
# backtester tool
# ----------------------------------------------------------------------
add_nbbo_tool(run_backtester
  src/run_backtester.cpp
  src/backtester.cpp
  src/strategy_config.cpp
  src/pnl_aggregator.cpp
)

# Backtester also needs histogram
target_link_libraries(run_backtester
  PRIVATE
    nbbo_histogram
)

# summarize trades
add_nbbo_tool(summarize_trades
  src/summarize_trades.cpp
)

target_link_libraries(run_backtester
  PRIVATE
    nbbo_core
)
target_compile_features(run_backtester PRIVATE cxx_std_23)

# ------------------------------------------------------------------------------
# IPO/LTO when available
# ------------------------------------------------------------------------------
include(CheckIPOSupported)
check_ipo_supported(RESULT ipo_ok OUTPUT ipo_out)
if (ipo_ok)
  foreach(tgt nbbo_pipeline clean_mid_spikes build_events build_histogram run_backtester)
    if (TARGET ${tgt})
      set_property(TARGET ${tgt} PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
    endif()
  endforeach()
endif()

message(STATUS "Arrow target:   ${ARROW_TGT}")
message(STATUS "Parquet target: ${PARQUET_TGT}")
message(STATUS "Build type:     ${CMAKE_BUILD_TYPE}")
