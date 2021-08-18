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

#include <glog/logging.h>
#include <gtest/gtest.h>
#include <iostream>
#include "velox/experimental/codegen/library_loader/NativeLibraryLoader.h"

TEST(NativeLoaderTest, testCompatibleTypeInfo) {
  using namespace facebook::velox::codegen::native_loader;
  NativeLibraryLoader loader;
  uint64_t initArgument;
  auto libraryPtr = loader.loadLibrary(std::string(TYPEINFO), &initArgument);
  auto getStringTypeInfoAddress =
      loader.getFunction<const void* (*)()>("stringTypeInfo", libraryPtr);
  ASSERT_EQ(getStringTypeInfoAddress(), &typeid(std::string));
}

TEST(NativeLoaderTest, testWrongPath) {
  using namespace facebook::velox::codegen::native_loader;
  NativeLibraryLoader loader;
  EXPECT_THROW(
      loader.loadLibrary(std::string("non-existng")),
      NativeLibraryLoaderException);
}

TEST(NativeLoaderTest, illFormedLibrary) {
  using namespace facebook::velox::codegen::native_loader;
  NativeLibraryLoader loader;
  EXPECT_THROW(loader.loadLibrary(BADLIBPATH), NativeLibraryLoaderException);
};

TEST(NativeLoaderTest, testWrongFunctionName) {
  using namespace facebook::velox::codegen::native_loader;
  NativeLibraryLoader loader;
  uint64_t initArgument;
  auto libraryPtr = loader.loadLibrary(
      std::string(GOODLIBPATH),
      &initArgument); // GOODLIBPATH defined in cmake file
  using InitType = void (*)();
  EXPECT_THROW(
      loader.getFunction<InitType>("init", libraryPtr),
      NativeLibraryLoaderException);
}

TEST(NativeLoaderTest, testWrongFunctionSignature) {
  using namespace facebook::velox::codegen::native_loader;
  NativeLibraryLoader loader;
  uint64_t initArgument;
  auto libraryPtr = loader.loadLibrary(
      std::string(GOODLIBPATH),
      &initArgument); // GOODLIBPATH defined in cmake file
  using wrongType1 =
      int (*)(int* a, void* b); // expected type is int (*)(void* a, void* b)
  EXPECT_THROW(
      loader.getFunction<wrongType1>("function1", libraryPtr),
      NativeLibraryLoaderException);
}

TEST(NativeLoaderTest, testCallResult) {
  using namespace facebook::velox::codegen::native_loader;
  NativeLibraryLoader loader;
  uint64_t initArgument;
  auto libraryPtr = loader.loadLibrary(
      std::string(GOODLIBPATH),
      &initArgument); // GOODLIBPATH defined in cmake file
  using Type1 = int (*)(void* a, void* b);
  using Type2 = int (*)(void* a, void* b);

  auto function1 = loader.getFunction<Type1>("function1", libraryPtr);
  auto callResult1 =
      function1(reinterpret_cast<void*>(1), reinterpret_cast<void*>(2));
  auto function2 = loader.getFunction<Type2>("function2", libraryPtr);
  auto callResult2 =
      function2(reinterpret_cast<void*>(1), reinterpret_cast<void*>(2));
  ASSERT_EQ(callResult1, 3);
  ASSERT_EQ(callResult2, -1);
}

TEST(NativeLoaderTest, testlibraryUnload) {
  using namespace facebook::velox::codegen::native_loader;
  NativeLibraryLoader loader;
  uint64_t initArgument = 45;
  auto libraryPtr = loader.loadLibrary(
      std::string(GOODLIBPATH),
      &initArgument); // GOODLIBPATH defined in cmake file
  uint64_t releaseArgument = 0;
  loader.unLoadLibrary(libraryPtr, &releaseArgument);
  ASSERT_EQ(initArgument, releaseArgument);
}
