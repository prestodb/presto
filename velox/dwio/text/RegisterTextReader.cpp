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

#include "velox/dwio/text/RegisterTextReader.h"
#include "velox/dwio/text/reader/TextReader.h"

namespace facebook::velox::text {

std::unique_ptr<dwio::common::Reader> TextReaderFactory::createReader(
    std::unique_ptr<BufferedInput> input,
    const ReaderOptions& options) {
  return std::make_unique<text::TextReader>(options, std::move(input));
}

void registerTextReaderFactory() {
  registerReaderFactory(std::make_shared<TextReaderFactory>());
}

void unregisterTextReaderFactory() {
  unregisterReaderFactory(dwio::common::FileFormat::TEXT);
}

} // namespace facebook::velox::text
