/*
 * Copyright (c) Facebook, Inc. and its affiliates.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

#include "velox/dwio/parquet/tests/reader/ParquetReaderBenchmark.h"

using namespace facebook::velox;
using namespace facebook::velox::dwio;
using namespace facebook::velox::dwio::common;
using namespace facebook::velox::parquet;
using namespace facebook::velox::parquet::test;
using namespace facebook::velox::test;

#define PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, _null_) \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_5k_dict,         \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      5000,                                                               \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_5k_plain,        \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      5000,                                                               \
      true);                                                              \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_10k_dict,        \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      10000,                                                              \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_10k_Plain,       \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      10000,                                                              \
      true);                                                              \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_20k_dict,        \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      20000,                                                              \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_20k_plain,       \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      20000,                                                              \
      true);                                                              \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_50k_dict,        \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      50000,                                                              \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_50k_plain,       \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      50000,                                                              \
      true);                                                              \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_100k_dict,       \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      100000,                                                             \
      false);                                                             \
  BENCHMARK_NAMED_PARAM(                                                  \
      run,                                                                \
      _name_##_Filter_##_filter_##_Nulls_##_null_##_next_100k_plain,      \
      #_name_,                                                            \
      _type_,                                                             \
      _filter_,                                                           \
      _null_,                                                             \
      100000,                                                             \
      true);                                                              \
  BENCHMARK_DRAW_LINE();

#define PARQUET_BENCHMARKS_FILTERS(_type_, _name_, _filter_)    \
  PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 0)  \
  PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 20) \
  PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 50) \
  PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 70) \
  PARQUET_BENCHMARKS_FILTER_NULLS(_type_, _name_, _filter_, 100)

#define PARQUET_BENCHMARKS(_type_, _name_)        \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 0)   \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 20)  \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 50)  \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 70)  \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 100) \
  BENCHMARK_DRAW_LINE();

#define PARQUET_BENCHMARKS_NO_FILTER(_type_, _name_) \
  PARQUET_BENCHMARKS_FILTERS(_type_, _name_, 100)    \
  BENCHMARK_DRAW_LINE();

PARQUET_BENCHMARKS(DECIMAL(18, 3), ShortDecimalType);
PARQUET_BENCHMARKS(DECIMAL(38, 3), LongDecimalType);
PARQUET_BENCHMARKS(VARCHAR(), Varchar);

PARQUET_BENCHMARKS(BIGINT(), BigInt);
PARQUET_BENCHMARKS(DOUBLE(), Double);
PARQUET_BENCHMARKS_NO_FILTER(MAP(BIGINT(), BIGINT()), Map);
PARQUET_BENCHMARKS_NO_FILTER(ARRAY(BIGINT()), List);

// TODO: Add all data types

int main(int argc, char** argv) {
  folly::Init init{&argc, &argv};
  memory::MemoryManager::initialize({});
  folly::runBenchmarks();
  return 0;
}
