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

#include "velox/functions/remote/if/GetSerde.h"
#include "velox/serializers/PrestoSerializer.h"
#include "velox/serializers/UnsafeRowSerializer.h"

namespace facebook::velox::functions {

std::unique_ptr<VectorSerde> getSerde(const remote::PageFormat& format) {
  switch (format) {
    case remote::PageFormat::PRESTO_PAGE:
      return std::make_unique<serializer::presto::PrestoVectorSerde>();

    case remote::PageFormat::SPARK_UNSAFE_ROW:
      return std::make_unique<serializer::spark::UnsafeRowVectorSerde>();
  }
  VELOX_UNREACHABLE();
}
} // namespace facebook::velox::functions
