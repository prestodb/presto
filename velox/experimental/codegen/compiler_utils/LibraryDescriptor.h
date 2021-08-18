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
#include <filesystem>
#include <fstream>
#include <sstream>
#include <string>
#include <variant>
#include <vector>
#include "fmt/format.h"
#include "velox/experimental/codegen/proto/codegen_proto.pb.h"
#include "velox/experimental/codegen/transform/utils/ranges_utils.h"

namespace facebook::velox::codegen::compiler_utils {
// TODO: Use Aggregate initialization C++20 instead of fluent interface
// Information use to produce compilation and linking commands.
struct LibraryDescriptor {
  std::string name; // Library name, not used in command line
  std::vector<std::filesystem::path> includePath; // -I
  std::vector<std::filesystem::path> systemIncludePath; //-isystem
  std::vector<std::filesystem::path> linkerSearchPath; // -L
  std::vector<std::string> libraryNames; // -l

  // Allow using LibraryDescriptor in set, assumes that name is unique
  bool operator<(const LibraryDescriptor& rhs) const {
    return name < rhs.name;
  }

  // Used to referenced linker object not in the default search paths.
  std::vector<std::filesystem::path> additionalLinkerObject;

  /// Converts a LibraryDescriptProto to LibraryDescriptor.
  static LibraryDescriptor fromProto(
      const proto::LibraryDescriptorProto& libraryDescriptorProto) {
    using namespace google::protobuf;
    using namespace proto;

    return LibraryDescriptor()
        .withName(libraryDescriptorProto.name())
        .withIncludePath(std::vector<std::filesystem::path>(
            libraryDescriptorProto.includepath().begin(),
            libraryDescriptorProto.includepath().end()))
        .withSystemIncludePath(std::vector<std::filesystem::path>(
            libraryDescriptorProto.systemincludepath().begin(),
            libraryDescriptorProto.systemincludepath().end()))
        .withLinkerSearchPath(std::vector<std::filesystem::path>(
            libraryDescriptorProto.linkersearchpath().begin(),
            libraryDescriptorProto.linkersearchpath().end()))
        .withLibraryNames(std::vector<std::string>(
            libraryDescriptorProto.librarynames().begin(),
            libraryDescriptorProto.librarynames().end()))
        .withAdditionalLinkerObject(std::vector<std::filesystem::path>(
            libraryDescriptorProto.additionallinkerobject().begin(),
            libraryDescriptorProto.additionallinkerobject().end()));
  }

  static LibraryDescriptor fromJsonFile(std::filesystem::path jsonFilePath) {
    /// Use std library instead of velox::common::file::LocalReadFile because
    /// LocalReadFile does not automatically decode the binary representation
    /// to string representation in memory
    std::stringstream ss;
    ss << std::ifstream(jsonFilePath.string()).rdbuf();
    return loadFromJson(ss.str());
  }

  /// Converts a Json string to LibraryDescriptor.
  static LibraryDescriptor loadFromJson(std::string_view jsonString) {
    proto::LibraryDescriptorProto libraryDescriptorProto;
    auto status = google::protobuf::util::JsonStringToMessage(
        jsonString.data(), &libraryDescriptorProto);
    if (!status.ok()) {
      throw std::logic_error(
          "Failed to load LibraryDescriptor from JsonString: " +
          status.message().as_string());
    }
    return fromProto(libraryDescriptorProto);
  }

  /// Sets the values of a LibraryDescriptorProto to that of a
  /// LibraryDescriptor.
  static proto::LibraryDescriptorProto& setProto(
      const LibraryDescriptor& libraryDescriptor,
      proto::LibraryDescriptorProto& libraryDescriptorProto) {
    using namespace google::protobuf;
    using namespace proto;

    libraryDescriptorProto.set_name(libraryDescriptor.name);
    *libraryDescriptorProto.mutable_includepath() = {
        libraryDescriptor.includePath.begin(),
        libraryDescriptor.includePath.end()};
    *libraryDescriptorProto.mutable_linkersearchpath() = {
        libraryDescriptor.linkerSearchPath.begin(),
        libraryDescriptor.linkerSearchPath.end()};
    *libraryDescriptorProto.mutable_librarynames() = {
        libraryDescriptor.libraryNames.begin(),
        libraryDescriptor.libraryNames.end()};
    *libraryDescriptorProto.mutable_additionallinkerobject() = {
        libraryDescriptor.additionalLinkerObject.begin(),
        libraryDescriptor.additionalLinkerObject.end()};

    return libraryDescriptorProto;
  }

  /// Converts a LibraryDescriptor to LibraryDescriptorProto.
  static proto::LibraryDescriptorProto asProto(
      const LibraryDescriptor& libraryDescriptor) {
    proto::LibraryDescriptorProto libraryDescriptorProto;
    return setProto(libraryDescriptor, libraryDescriptorProto);
  }

  /// Converts a LibraryDescriptor to its Json representation.
  static std::string formatAsJson(const LibraryDescriptor& libraryDescriptor) {
    std::string output;
    auto status = google::protobuf::util::MessageToJsonString(
        asProto(libraryDescriptor), &output);
    if (!status.ok()) {
      throw std::logic_error(
          "Failed to format LibraryDescriptor as Json" +
          status.message().as_string());
    }

    return output;
  }

  LibraryDescriptor& withName(const std::string& newName) {
    name = newName;
    return *this;
  }

  LibraryDescriptor& withIncludePath(
      const std::vector<std::filesystem::path>& path) {
    includePath = path;
    return *this;
  }

  LibraryDescriptor& withSystemIncludePath(
      const std::vector<std::filesystem::path>& path) {
    systemIncludePath = path;
    return *this;
  }

  LibraryDescriptor& withLinkerSearchPath(
      const std::vector<std::filesystem::path>& path) {
    linkerSearchPath = path;
    return *this;
  }

  LibraryDescriptor& withLibraryNames(const std::vector<std::string>& names) {
    libraryNames = names;
    return *this;
  }

  LibraryDescriptor& withAdditionalLinkerObject(
      const std::vector<std::filesystem::path>& path) {
    additionalLinkerObject = path;
    return *this;
  }
};
} // namespace facebook::velox::codegen::compiler_utils
