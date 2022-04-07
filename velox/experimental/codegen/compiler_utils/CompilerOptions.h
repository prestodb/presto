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
#include <string>
#include "fmt/format.h"
#include "velox/experimental/codegen/compiler_utils/LibraryDescriptor.h"
#include "velox/experimental/codegen/proto/ProtoUtils.h"
#include "velox/experimental/codegen/proto/codegen_proto.pb.h"

namespace facebook::velox::codegen::compiler_utils {

/// Compiler options
struct CompilerOptions {
  CompilerOptions() = default;
  CompilerOptions(const CompilerOptions& options) = default;
  CompilerOptions& operator=(const CompilerOptions& other) = default;

  std::string optimizationLevel;
  std::vector<std::string> extraCompileOptions;
  std::vector<std::string> extraLinkOptions;
  std::vector<LibraryDescriptor> defaultLibraries;
  std::filesystem::path compilerPath;
  std::optional<std::filesystem::path> linker;
  std::optional<std::filesystem::path> formatterPath;
  std::filesystem::path tempDirectory;

  /// Converts a CompilerOptionsProto to a CompilerOptions
  static CompilerOptions fromProto(
      const proto::CompilerOptionsProto& compilerOptionsProto) {
    using namespace google::protobuf;
    using namespace proto;

    std::vector<LibraryDescriptor> defaultLibraries;
    std::transform(
        compilerOptionsProto.defaultlibraries().begin(),
        compilerOptionsProto.defaultlibraries().end(),
        std::back_inserter(defaultLibraries),
        [](const LibraryDescriptorProto& libraryDescriptorProto) {
          return LibraryDescriptor::fromProto(libraryDescriptorProto);
        });

    auto compilerOptions =
        CompilerOptions()
            .withOptimizationLevel(compilerOptionsProto.optimizationlevel())
            .withExtraCompileOptions(std::vector<std::string>(
                compilerOptionsProto.extracompileoption().begin(),
                compilerOptionsProto.extracompileoption().end()))
            .withExtraLinkOptions(std::vector<std::string>(
                compilerOptionsProto.extralinkoptions().begin(),
                compilerOptionsProto.extralinkoptions().end()))
            .withDefaultLibraries(defaultLibraries)
            .withCompilerPath(compilerOptionsProto.compilerpath())
            .withTempDirectory(compilerOptionsProto.tempdirectory());

    if (!compilerOptionsProto.linker().empty()) {
      compilerOptions.withLinker(compilerOptionsProto.linker());
    }
    if (!compilerOptionsProto.formatterpath().empty()) {
      compilerOptions.withFormatterPath(compilerOptionsProto.formatterpath());
    }
    return compilerOptions;
  }

  static CompilerOptions fromJsonFile(std::filesystem::path jsonFilePath) {
    /// Use std library instead of velox::common::file::LocalReadFile because
    /// LocalReadFile does not automatically decode the binary representation
    /// to string representation in memory
    if (!std::filesystem::exists(jsonFilePath)) {
      throw std::logic_error("Missing  JsonFile  : " + jsonFilePath.string());
    };
    std::stringstream ss;
    ss << std::ifstream(jsonFilePath.string()).rdbuf();
    return loadFromJson(ss.str());
  }

  /// Converts a Json string to a CompilerOptions
  static CompilerOptions loadFromJson(std::string_view jsonString) {
    proto::CompilerOptionsProto compilerOptionsProto;
    auto status = google::protobuf::util::JsonStringToMessage(
        jsonString.data(), &compilerOptionsProto);
    if (!status.ok()) {
      throw std::logic_error(
          "Failed to load CompilerOptions from JsonString: " +
          status.message().as_string());
    }
    return fromProto(compilerOptionsProto);
  }

  /// Sets the values of a CompilerOptionsProto using values from
  /// CompilerOptions
  static proto::CompilerOptionsProto& setProto(
      const CompilerOptions& compilerOptions,
      proto::CompilerOptionsProto& compilerOptionsProto) {
    using namespace google::protobuf;
    using namespace proto;

    compilerOptionsProto.set_optimizationlevel(
        compilerOptions.optimizationLevel);
    // Reassignment using initializer list requires c++11
    *compilerOptionsProto.mutable_extracompileoption() = {
        compilerOptions.extraCompileOptions.begin(),
        compilerOptions.extraCompileOptions.end()};
    *compilerOptionsProto.mutable_extralinkoptions() = {
        compilerOptions.extraLinkOptions.begin(),
        compilerOptions.extraLinkOptions.end()};

    std::for_each(
        compilerOptions.defaultLibraries.begin(),
        compilerOptions.defaultLibraries.end(),
        [&](const LibraryDescriptor& libraryDescriptor) {
          LibraryDescriptorProto* libraryDescriptorProto =
              compilerOptionsProto.add_defaultlibraries();
          LibraryDescriptor::setProto(
              libraryDescriptor, *libraryDescriptorProto);
        });

    compilerOptionsProto.set_compilerpath(compilerOptions.compilerPath);
    compilerOptionsProto.set_linker(compilerOptions.linker.value_or(""));
    compilerOptionsProto.set_formatterpath(
        compilerOptions.formatterPath.value_or(""));
    compilerOptionsProto.set_tempdirectory(compilerOptions.tempDirectory);

    return compilerOptionsProto;
  }

  /// Converts a CompilerOptions to CompilerOptionsProto.
  static proto::CompilerOptionsProto asProto(
      const CompilerOptions& compilerOptions) {
    proto::CompilerOptionsProto compilerOptionsProto;
    return setProto(compilerOptions, compilerOptionsProto);
  }

  /// Converts a CompilerOptions to its Json representation.
  static std::string formatAsJson(const CompilerOptions& compilerOptions) {
    std::string output;
    auto status = google::protobuf::util::MessageToJsonString(
        asProto(compilerOptions), &output);
    if (!status.ok()) {
      throw std::logic_error(
          "Failed to format CompilerOptions as Json" +
          status.message().as_string());
    }
    return output;
  }

  /// Fluent interface.

  CompilerOptions& withOptimizationLevel(const std::string& opt) {
    optimizationLevel = opt;
    return *this;
  }

  CompilerOptions& withExtraCompileOptions(
      const std::vector<std::string>& extraOptions) {
    extraCompileOptions = extraOptions;
    return *this;
  }
  CompilerOptions& withExtraLinkOptions(
      const std::vector<std::string>& linkOptions) {
    extraLinkOptions = linkOptions;
    return *this;
  }

  CompilerOptions& withDefaultLibraries(
      const std::vector<LibraryDescriptor>& libraries) {
    defaultLibraries = libraries;
    return *this;
  }

  CompilerOptions& withCompilerPath(const std::filesystem::path& path) {
    compilerPath = path;
    return *this;
  }

  CompilerOptions& withTempDirectory(const std::filesystem::path& temp) {
    tempDirectory = temp;
    return *this;
  }

  CompilerOptions& withLinker(const std::filesystem::path& path) {
    linker = path;
    return *this;
  }

  CompilerOptions& withFormatterPath(const std::filesystem::path& path) {
    formatterPath = path;
    return *this;
  }
};
} // namespace facebook::velox::codegen::compiler_utils
