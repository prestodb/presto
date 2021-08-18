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

#include <dlfcn.h>
#include <gtest/gtest.h>
#include <fstream>
#include <iostream>
#include <regex>
#include "boost/filesystem.hpp"
#include "velox/experimental/codegen/compiler_utils/Compiler.h"
#include "velox/experimental/codegen/compiler_utils/tests/definitions.h"
#include "velox/experimental/codegen/external_process/Filesystem.h"
#include "velox/experimental/codegen/library_loader/NativeLibraryLoader.h"

namespace facebook::velox::codegen::compiler_utils::test {

TEST(CompilerUtils, Command) {
  auto testOptions = CompilerOptions()
                         .withCompilerPath("/usr/bin/clang")
                         .withOptimizationLevel("-O3")
                         .withExtraCompileOptions({"-std=c++17"})
                         .withFormatterPath("/usr/local/bin/clang-format");

  DefaultScopedTimer::EventSequence eventSequence;
  Compiler compiler(testOptions, eventSequence);
  auto command = compiler.compileCommand({}, "/user/test.cpp", "user.o");
  auto expected =
      "/usr/bin/clang\n"
      "-std=c++17\n"
      "-O3\n"
      "-c\n"
      "/user/test.cpp\n"
      "-o\n"
      "user.o";
  ASSERT_EQ(command.executablePath, "/usr/bin/clang");
  ASSERT_EQ(command.arguments.size(), 6);
  ASSERT_EQ(command.toString(), expected);
}

struct LibraryDescriptorProtoTest : public ::testing::Test {
  void SetUp() override {
    testLibraryDescriptor.withName("libraryDescriptorName")
        .withIncludePath({"/usr/local/include/"})
        .withLinkerSearchPath({"/usr/local/ld.lld"})
        .withLibraryNames({"libName1", "libName2", "libName3"})
        .withAdditionalLinkerObject(
            {"/usr/local/lib/lib1", "/usr/local/lib/lib2"});
  }
  LibraryDescriptor testLibraryDescriptor;
};

TEST_F(LibraryDescriptorProtoTest, formatLoadJson) {
  std::string libraryDescriptorJson =
      proto::proto_utils::ProtoUtils<proto::LibraryDescriptorProto>::
          formatAsJson(LibraryDescriptor::asProto(testLibraryDescriptor));

  proto::LibraryDescriptorProto libraryDescriptorProto =
      proto::proto_utils::ProtoUtils<proto::LibraryDescriptorProto>::
          loadProtoFromJson(libraryDescriptorJson);

  LibraryDescriptor newLibraryDescriptor =
      LibraryDescriptor::fromProto(libraryDescriptorProto);

  ASSERT_EQ(testLibraryDescriptor.name, newLibraryDescriptor.name);
  ASSERT_EQ(
      testLibraryDescriptor.includePath, newLibraryDescriptor.includePath);
  ASSERT_EQ(
      testLibraryDescriptor.linkerSearchPath,
      newLibraryDescriptor.linkerSearchPath);
  ASSERT_EQ(
      testLibraryDescriptor.libraryNames, newLibraryDescriptor.libraryNames);
  ASSERT_EQ(
      testLibraryDescriptor.additionalLinkerObject,
      newLibraryDescriptor.additionalLinkerObject);
}

TEST_F(LibraryDescriptorProtoTest, malformedJson) {
  std::string malformedJson = "{field: data}";
  EXPECT_THROW(
      proto::proto_utils::ProtoUtils<
          proto::LibraryDescriptorProto>::loadProtoFromJson(malformedJson),
      std::logic_error);
}

TEST_F(LibraryDescriptorProtoTest, loadFromFile) {
  auto tempPath = boost::filesystem::unique_path(
                      fmt::format(
                          "{}/%%%%-%%%%",
                          boost::filesystem::temp_directory_path().string()))
                      .string();
  std::ofstream ostream(tempPath);
  auto jsonString =
      proto::proto_utils::ProtoUtils<proto::LibraryDescriptorProto>::
          formatAsJson(LibraryDescriptor::asProto(testLibraryDescriptor));

  ostream.write(jsonString.data(), jsonString.size());
  ostream.close();

  EXPECT_NO_THROW(
      auto newLibraryDescriptorProto = proto::proto_utils::ProtoUtils<
          proto::LibraryDescriptorProto>::loadProtoFromFile(tempPath);

      ASSERT_EQ(
          proto::proto_utils::ProtoUtils<proto::LibraryDescriptorProto>::
              formatAsJson(newLibraryDescriptorProto),
          jsonString););
}

struct CompilerOptionsProtoTest : LibraryDescriptorProtoTest {
  void SetUp() override {
    LibraryDescriptorProtoTest::SetUp();
    testCompilerOptions_ = testCompilerOptions().withDefaultLibraries(
        {testLibraryDescriptor, testLibraryDescriptor, testLibraryDescriptor});
  }
  CompilerOptions testCompilerOptions_;
};

TEST_F(CompilerOptionsProtoTest, malformedJson) {
  std::string malformedJson = "{field: data}";
  EXPECT_THROW(
      proto::proto_utils::ProtoUtils<
          proto::CompilerOptionsProto>::loadProtoFromJson(malformedJson),
      std::logic_error);
}

TEST_F(CompilerOptionsProtoTest, loadFromFile) {
  auto tempPath = boost::filesystem::unique_path(
                      fmt::format(
                          "{}/%%%%-%%%%",
                          boost::filesystem::temp_directory_path().string()))
                      .string();
  std::ofstream ostream(tempPath);
  auto jsonString =
      proto::proto_utils::ProtoUtils<proto::CompilerOptionsProto>::formatAsJson(
          CompilerOptions::asProto(testCompilerOptions_));
  ostream.write(jsonString.data(), jsonString.size());
  ostream.close();
  EXPECT_NO_THROW(
      auto newCompilerOptionsProto = proto::proto_utils::ProtoUtils<
          proto::CompilerOptionsProto>::loadProtoFromFile(tempPath);

      ASSERT_EQ(
          proto::proto_utils::ProtoUtils<proto::CompilerOptionsProto>::
              formatAsJson(newCompilerOptionsProto),
          jsonString););
}

TEST_F(CompilerOptionsProtoTest, formatLoadJson) {
  std::string compilerOptionsJson =
      proto::proto_utils::ProtoUtils<proto::CompilerOptionsProto>::formatAsJson(
          CompilerOptions::asProto(testCompilerOptions_));

  proto::CompilerOptionsProto newCompilerOptionsProto = proto::proto_utils::
      ProtoUtils<proto::CompilerOptionsProto>::loadProtoFromJson(
          CompilerOptions::formatAsJson(testCompilerOptions_));
  CompilerOptions newCompilerOptions =
      CompilerOptions::loadFromJson(compilerOptionsJson);

  ASSERT_EQ(
      testCompilerOptions_.optimizationLevel,
      newCompilerOptions.optimizationLevel);
  ASSERT_EQ(
      testCompilerOptions_.extraCompileOptions,
      newCompilerOptions.extraCompileOptions);
  ASSERT_EQ(
      testCompilerOptions_.extraLinkOptions,
      newCompilerOptions.extraLinkOptions);
  ASSERT_EQ(newCompilerOptions.defaultLibraries.size(), 3);
  auto newLibraryDescriptor = newCompilerOptions.defaultLibraries[0];
  ASSERT_EQ(testLibraryDescriptor.name, newLibraryDescriptor.name);
  ASSERT_EQ(
      testLibraryDescriptor.includePath, newLibraryDescriptor.includePath);
  ASSERT_EQ(
      testLibraryDescriptor.linkerSearchPath,
      newLibraryDescriptor.linkerSearchPath);
  ASSERT_EQ(
      testLibraryDescriptor.libraryNames, newLibraryDescriptor.libraryNames);
  ASSERT_EQ(
      testLibraryDescriptor.additionalLinkerObject,
      newLibraryDescriptor.additionalLinkerObject);
  ASSERT_EQ(testCompilerOptions_.compilerPath, newCompilerOptions.compilerPath);
  ASSERT_EQ(testCompilerOptions_.linker, newCompilerOptions.linker);
  ASSERT_EQ(
      testCompilerOptions_.formatterPath, newCompilerOptions.formatterPath);
  ASSERT_EQ(
      testCompilerOptions_.tempDirectory, newCompilerOptions.tempDirectory);
}

TEST(Compiler, ExternalLibraries) {
  using facebook::velox::codegen::compiler_utils::Compiler;
  using facebook::velox::codegen::compiler_utils::CompilerOptions;

  using facebook::velox::codegen::compiler_utils::filesystem::PathGenerator;

  auto libraries = std::vector<LibraryDescriptor>{
      LibraryDescriptor()
          .withName("format")
          .withIncludePath({FMT_INCLUDE})
          .withAdditionalLinkerObject({FMT_LIB})};

  auto sourceCode = R"a(
  #include <string>
  #include "fmt/format.h"
  extern "C" {
  std::string f() {
    return fmt::format("test {}",2);
  }
  }
  )a";

  auto testOptions = testCompilerOptions();

  DefaultScopedTimer::EventSequence eventSequence;
  Compiler compiler(testOptions, eventSequence);
  PathGenerator pathGenerator;
  auto generated = pathGenerator.tempPath("compile", ".cpp");
  auto binary = pathGenerator.tempPath("linkObject", ".o");
  auto sharedObject = pathGenerator.tempPath("shared", ".so");

  std::ofstream compileFile(generated);
  compileFile << sourceCode << std::endl;
  compileFile.close();

  compiler.compile({libraries}, generated, binary);
  compiler.link({libraries}, {binary}, sharedObject);

  auto lib =
      native_loader::NativeLibraryLoader::loadLibraryInternal(sharedObject);

  auto func = (std::string(*)())dlsym(lib, "f");
  ASSERT_EQ(func(), "test 2");
}

TEST(Compiler, CompileAndLink) {
  auto sourceCode1 = R"a(
  extern "C" {
  int f() {
    return 24;
  };
  }
  )a";

  auto sourceCode2 = R"a(
  extern "C" {
  int g() {
    return 32;
  };
  }
  )a";

  auto options = testCompilerOptions();

  DefaultScopedTimer::EventSequence eventSequence;

  Compiler compiler(options, eventSequence);

  auto binary = compiler.compileString({}, sourceCode1);
  ASSERT_TRUE(std::filesystem::exists(binary));
  ASSERT_GT(std::filesystem::file_size(binary), 0);

  auto binary2 = compiler.compileString({}, sourceCode2);
  ASSERT_TRUE(std::filesystem::exists(binary2));
  ASSERT_GT(std::filesystem::file_size(binary2), 0);

  auto sharedObject = compiler.link({}, {binary, binary2});
  ASSERT_TRUE(std::filesystem::exists(sharedObject));
  ASSERT_GT(std::filesystem::file_size(sharedObject), 0);

  auto libraryPtr =
      native_loader::NativeLibraryLoader::loadLibraryInternal(sharedObject);

  auto g = (int (*)())dlsym(libraryPtr, "g");

  ASSERT_EQ(g(), 32);

  auto f = (int (*)())dlsym(libraryPtr, "f");
  ASSERT_EQ(dlerror(), nullptr);
  ASSERT_EQ(f(), 24);
};
} // namespace facebook::velox::codegen::compiler_utils::test
