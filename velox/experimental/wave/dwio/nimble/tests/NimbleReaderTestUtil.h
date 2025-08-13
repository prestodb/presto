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

#pragma once

#include <cuda_runtime.h> // @manual
#include <fmt/format.h>
#include <gflags/gflags.h>
#include <gtest/gtest.h>

#include "dwio/nimble/encodings/EncodingSelectionPolicy.h"
#include "dwio/nimble/tablet/TabletReader.h"
#include "velox/dwio/common/Statistics.h"
#include "velox/exec/fuzzer/ReferenceQueryRunner.h"
#include "velox/experimental/wave/dwio/ColumnReader.h"
#include "velox/experimental/wave/dwio/nimble/NimbleFileFormat.h"
#include "velox/experimental/wave/dwio/nimble/NimbleFormatData.h"
#include "velox/type/Filter.h"

namespace facebook::velox::wave {
using nimble::CompressionOptions;
using nimble::EncodingType;
using nimble::StreamLoader;
struct FilterSpec {
  std::string name;
  std::shared_ptr<common::Filter> filter;
  bool keepValues;

  std::string toString() const {
    switch (filter->kind()) {
      case common::FilterKind::kBigintRange: {
        auto bigIntRangeFilter =
            std::dynamic_pointer_cast<common::BigintRange>(this->filter);
        return fmt::format(
            "{} BETWEEN {} AND {}",
            name,
            bigIntRangeFilter->lower(),
            bigIntRangeFilter->upper());
      }
      default:
        VELOX_NYI("Not supported filter type");
    }
  }
};

class TestNimbleReader {
 public:
  TestNimbleReader(
      memory::MemoryPool* pool,
      RowVectorPtr input,
      std::vector<std::unique_ptr<StreamLoader>>& streamLoaders,
      const std::vector<FilterSpec>& filters);
  RowVectorPtr read();

 private:
  memory::MemoryPool* pool_{nullptr};
  RowVectorPtr input_{nullptr};
  std::unique_ptr<GpuArena> deviceArena_{nullptr};
  std::shared_ptr<common::ScanSpec> scanSpec_{nullptr};
  std::unique_ptr<NimbleStripe> stripe_{nullptr};
  std::unique_ptr<NimbleFormatParams> formatParams_{nullptr};
  std::unique_ptr<ColumnReader> reader_{nullptr};
  dwio::common::ColumnReaderStatistics stats_;
  io::IoStatistics ioStats_;
  FileInfo fileInfo_;
  OperatorStateMap operandStateMap_;
  std::unique_ptr<WaveStream> waveStream_{nullptr};
  std::unique_ptr<ReadStream> readStream_{nullptr};
  std::vector<int32_t> nonNullOutputChildrenIds_;
  bool earlyStop_{false};

  GpuArena& deviceArena() {
    return *deviceArena_;
  }
};

class NimbleReaderVerifier {
 public:
  explicit NimbleReaderVerifier(
      std::unique_ptr<exec::test::ReferenceQueryRunner> referenceQueryRunner =
          nullptr)
      : referenceQueryRunner_{std::move(referenceQueryRunner)} {}

  void verify(
      RowVectorPtr input,
      const std::vector<FilterSpec>& filters,
      RowVectorPtr actual);

 private:
  std::unique_ptr<exec::test::ReferenceQueryRunner> referenceQueryRunner_;
  void verifyWithQueryRunner(
      RowVectorPtr input,
      const std::vector<FilterSpec>& filters,
      RowVectorPtr actual);
};

// Helper function to create input by combining chunk vector groups
RowVectorPtr createInputFromChunkVectorGroups(
    const std::vector<std::vector<VectorPtr>>& chunkVectorGroups);

// Helper function to write multiple vectors to nimble files and get back all
// streamLoaders. A chunk vector group contains several chunked vectors
// (mapping to chunked streams in Nimble) that share the same chunk
// boundaries.
std::vector<std::unique_ptr<StreamLoader>> writeToNimbleAndGetStreamLoaders(
    memory::MemoryPool* pool,
    const std::vector<std::vector<VectorPtr>>& chunkVectorGroups,
    const std::vector<std::pair<EncodingType, float>>& readFactors,
    const CompressionOptions& compressionOptions);
} // namespace facebook::velox::wave
