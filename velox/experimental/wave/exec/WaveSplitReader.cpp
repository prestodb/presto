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

#include "velox/experimental/wave/exec/WaveSplitReader.h"

namespace facebook::velox::wave {
std::shared_ptr<WaveSplitReader> WaveSplitReader::create(
    const std::shared_ptr<velox::connector::ConnectorSplit>& split,
    const SplitReaderParams& params,
    const DefinesMap* defines) {
  for (auto& factory : factories_) {
    auto result = factory->create(split, params, defines);
    if (result) {
      return result;
    }
  }
  VELOX_FAIL("No WaveSplitReader for  {}", split->toString());
}

std::vector<std::unique_ptr<WaveSplitReaderFactory>>
    WaveSplitReader::factories_;

// static
void WaveSplitReader::registerFactory(
    std::unique_ptr<WaveSplitReaderFactory> factory) {
  factories_.push_back(std::move(factory));
}

} // namespace facebook::velox::wave
