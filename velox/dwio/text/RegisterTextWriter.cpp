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

#include "velox/dwio/text/writer/TextWriter.h"

namespace facebook::velox::text {

void registerTextWriterFactory() {
  dwio::common::registerWriterFactory(std::make_shared<TextWriterFactory>());
}

void unregisterTextWriterFactory() {
  dwio::common::unregisterWriterFactory(dwio::common::FileFormat::TEXT);
}

} // namespace facebook::velox::text
