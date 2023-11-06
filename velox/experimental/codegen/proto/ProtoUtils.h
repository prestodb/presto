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
#include <google/protobuf/util/json_util.h>
#include <fstream>

namespace facebook::velox::codegen::proto {

namespace proto_utils {
static std::stringstream readFromFile(const std::filesystem::path& filePath) {
  /// Use std library instead of velox::common::file::LocalReadFile because
  /// LocalReadFile does not automatically decode the binary representation
  /// to string representation in memory
  std::stringstream ss;
  ss << std::ifstream(filePath.string()).rdbuf();
  return ss;
}

template <typename ProtoType>
struct ProtoUtils {
  static ProtoType loadProtoFromFile(
      const std::filesystem::path& jsonFilePath) {
    return loadProtoFromJson(readFromFile(jsonFilePath).str());
  }

  static ProtoType loadProtoFromJson(std::string_view jsonString) {
    ProtoType proto;
    auto status =
        google::protobuf::util::JsonStringToMessage(jsonString.data(), &proto);
    if (!status.ok()) {
      throw std::logic_error(
          "Failed to load proto from JsonString: " +
          status.message().as_string());
    }
    return proto;
  }

  static std::string formatAsJson(const ProtoType& proto) {
    std::string output;
    auto status = google::protobuf::util::MessageToJsonString(proto, &output);
    if (!status.ok()) {
      throw std::logic_error(
          "Failed to format proto as Json" + status.message().as_string());
    }
    return output;
  }
};

}; // namespace proto_utils

} // namespace facebook::velox::codegen::proto
