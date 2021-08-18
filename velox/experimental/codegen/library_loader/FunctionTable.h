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
#include <map>
#include <memory>
#include <string>
#include <typeinfo>
#include <vector>

/// This header file is meant to be included in the generated libraries (.so)
/// and defines the interface expected by the library loader
///
/// Aggregate the information for each function symbol exported with a given
/// library The loader relies on typeInfo to provide a
/// type safe way to access defined functions
struct FunctionInfo {
  // Normaly we would use (void *) as to store a function address
  // but it seems that (void *) is actually not meant to store function
  // addresses only object addresses.
  uint64_t functionAddress;
  decltype(std::declval<std::type_info>().hash_code()) typeInfo;
};

/// Each library provide a function table listing all the exported functions
struct FunctionTable {
  std::map<std::string, FunctionInfo> exportedFunctions;
};

/// Register a function into a given function table. This compute the typeinfo
/// \tparam Func type used to compute typeinfo
/// \param functionName
/// \param function
/// \param functionTable
template <typename Func>
void registerFunction(
    const std::string& functionName,
    Func function,
    FunctionTable& functionTable) {
  using addressType = decltype(FunctionInfo::functionAddress);
  static_assert(sizeof(function) == sizeof(addressType));
  functionTable.exportedFunctions[functionName] = FunctionInfo{
      reinterpret_cast<addressType>(function), typeid(function).hash_code()};
};

// loader/library interface.
// Each libraries is expect to implement the interface below
extern "C" {

//
// Apple clang version 11.0.3 (clang-1103.0.32.59) is emitting :
// warning: 'getFunctionTable' has C-linkage specified,
// but returns incomplete type 'std::shared_ptr<FunctionTable>'
// which could be incompatible with C [-Wreturn-type-c-linkage]
//
// TODO: Investigate why std::shared_ptr here is seen as incomplete type
// TODO: Is it safe to have std::shared_ptr cross ABI boundaries?

#pragma clang diagnostic push
#pragma clang diagnostic ignored "-Wreturn-type-c-linkage"
std::shared_ptr<FunctionTable> getFunctionTable();
#pragma clang diagnostic pop

// TODO: Add interface to save/Restor the state of an expression
bool init(void* arg);
void release(void* arg);
}
