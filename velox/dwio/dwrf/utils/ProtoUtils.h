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

#include "velox/dwio/common/SeekableInputStream.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"
#include "velox/type/Type.h"

namespace facebook::velox::dwrf {

class ProtoUtils final {
 public:
  static void writeType(
      const Type& type,
      proto::Footer& footer,
      proto::Type* parent = nullptr);

  static std::shared_ptr<const Type> fromFooter(
      const proto::Footer& footer,
      std::function<bool(uint32_t)> selector = [](uint32_t) { return true; },
      uint32_t index = 0);

  // Deserialize proto from inputStream. Caller can optionally pass in the
  // object to serialize to.
  template <typename T>
  static std::unique_ptr<T> readProto(
      std::unique_ptr<dwio::common::SeekableInputStream> stream,
      std::unique_ptr<T> ret = nullptr) {
    if (!ret) {
      ret = std::make_unique<T>();
    }
    DWIO_ENSURE(
        ret->ParseFromZeroCopyStream(stream.get()),
        "Failed to parse proto from ",
        stream->getName());
    return ret;
  }

  // Reads 'stream' into the pre-existing proto 'ret'. Use this
  // instead of readProto when the proto is arena-allocated since
  // moving an arena proto via unique_ptr would be confusing since it
  // is not owned by the unique_ptr.
  template <typename T>
  static T* readProtoInto(
      std::unique_ptr<dwio::common::SeekableInputStream> stream,
      T* ret) {
    DWIO_ENSURE(ret);
    DWIO_ENSURE(
        ret->ParseFromZeroCopyStream(stream.get()),
        "Failed to parse proto from ",
        stream->getName());
    return ret;
  }
};

} // namespace facebook::velox::dwrf
