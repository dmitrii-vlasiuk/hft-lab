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


# ------------------------------------------------------------------------------
# nbbo_pipeline (existing main, if present)
# ------------------------------------------------------------------------------
if (EXISTS "${CMAKE_SOURCE_DIR}/src/nbbo_pipeline.cpp")
  add_executable(nbbo_pipeline src/nbbo_pipeline.cpp)
else()
  add_executable(nbbo_pipeline nbbo_pipeline.cpp)
endif()

target_link_libraries(
  nbbo_pipeline
  PRIVATE
    nbbo_core
    ZLIB::ZLIB
    Threads::Threads
)
target_compile_features(nbbo_pipeline PRIVATE cxx_std_23)

# ------------------------------------------------------------------------------
# clean_mid_spikes (denoiser CLI)
# ------------------------------------------------------------------------------
add_executable(clean_mid_spikes src/clean_mid_spikes.cpp)
target_link_libraries(
  clean_mid_spikes
  PRIVATE
    nbbo_core
    ZLIB::ZLIB
    Threads::Threads
)
target_compile_features(clean_mid_spikes PRIVATE cxx_std_23)
target_compile_options(clean_mid_spikes PRIVATE -O3 -pipe -fvisibility=hidden)

# # build_events
# add_executable(build_events src/build_events.cpp)
# target_link_libraries(
#   build_events
#   PRIVATE
#     nbbo_core
#     ZLIB::ZLIB
#     Threads::Threads
# )
# target_compile_features(build_events PRIVATE cxx_std_23)
# target_compile_options(build_events PRIVATE -O3 -pipe -fvisibility=hidden)

# ------------------------------------------------------------------------------
# IPO/LTO when available
# ------------------------------------------------------------------------------
include(CheckIPOSupported)
check_ipo_supported(RESULT ipo_ok OUTPUT ipo_out)
if (ipo_ok)
  set_property(TARGET nbbo_pipeline PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
  set_property(TARGET clean_mid_spikes PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
  if (TARGET nbbo_audit)
    set_property(TARGET nbbo_audit PROPERTY INTERPROCEDURAL_OPTIMIZATION TRUE)
  endif()
endif()

message(STATUS "Arrow target:   ${ARROW_TGT}")
message(STATUS "Parquet target: ${PARQUET_TGT}")
message(STATUS "Build type:     ${CMAKE_BUILD_TYPE}")
