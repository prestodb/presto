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

#include <fmt/core.h>
#include <cstdint>
#include <limits>
#include <string>

namespace facebook::velox::dwio::common {

constexpr uint32_t MAX_UINT32 = std::numeric_limits<uint32_t>::max();

class StreamIdentifier {
 public:
  StreamIdentifier() : id_(MAX_UINT32) {}

  explicit StreamIdentifier(int32_t id) : id_(id) {}

  virtual ~StreamIdentifier() = default;

  virtual int32_t getId() const {
    return id_;
  }

  virtual bool operator==(const StreamIdentifier& other) const {
    return id_ == other.id_;
  }

  virtual std::size_t hash() const {
    return std::hash<uint32_t>()(id_);
  }

  virtual std::string toString() const {
    return fmt::format("[id={}]", id_);
  }

  int32_t id_;
};

struct StreamIdentifierHash {
  std::size_t operator()(const StreamIdentifier& si) const {
    return si.hash();
  }
};

} // namespace facebook::velox::dwio::common
