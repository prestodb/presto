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

#include "velox/dwio/text/TextReader.h"

using namespace facebook::velox::dwio::common;
using namespace facebook::velox::common;

namespace facebook::velox::text {

/// TODO: Add implementation
std::unique_ptr<Reader> TextReaderFactory::createReader(
    std::unique_ptr<BufferedInput> input,
    const velox::dwio::common::ReaderOptions& options) {
  return nullptr;
}

void registerTextReaderFactory() {
  velox::dwio::common::registerReaderFactory(
      std::make_shared<TextReaderFactory>());
}

void unregisterTextReaderFactory() {
  velox::dwio::common::unregisterReaderFactory(
      velox::dwio::common::FileFormat::TEXT);
}

} // namespace facebook::velox::text
