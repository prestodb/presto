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

#include "velox/exec/Driver.h"
#include "velox/exec/Operator.h"

#include <gflags/gflags.h>

DECLARE_bool(velox_cudf_enabled);
DECLARE_string(velox_cudf_memory_resource);
DECLARE_bool(velox_cudf_debug);

namespace facebook::velox::cudf_velox {

static const std::string kCudfAdapterName = "cuDF";

// QueryConfig key. Enable or disable cudf in task level.
static const std::string kCudfEnabled = "cudf.enabled";

class CompileState {
 public:
  CompileState(const exec::DriverFactory& driverFactory, exec::Driver& driver)
      : driverFactory_(driverFactory), driver_(driver) {}

  exec::Driver& driver() {
    return driver_;
  }

  // Replaces sequences of Operators in the Driver given at construction with
  // cuDF equivalents. Returns true if the Driver was changed.
  bool compile(bool force_replace);

  const exec::DriverFactory& driverFactory_;
  exec::Driver& driver_;
};

class CudfOptions {
 public:
  static CudfOptions& getInstance() {
    static CudfOptions instance;
    return instance;
  }

  void setPrefix(const std::string& prefix) {
    prefix_ = prefix;
  }

  const std::string& prefix() const {
    return prefix_;
  }

  const bool cudfEnabled;
  const std::string cudfMemoryResource;
  // The initial percent of GPU memory to allocate for memory resource for one
  // thread.
  int memoryPercent;
  const bool force_replace;

  CudfOptions(bool force_repl)
      : cudfEnabled(FLAGS_velox_cudf_enabled),
        cudfMemoryResource(FLAGS_velox_cudf_memory_resource),
        memoryPercent(50),
        force_replace{force_repl},
        prefix_("") {}

 private:
  CudfOptions()
      : cudfEnabled(FLAGS_velox_cudf_enabled),
        cudfMemoryResource(FLAGS_velox_cudf_memory_resource),
        memoryPercent(50),
        force_replace{false},
        prefix_("") {}
  CudfOptions(const CudfOptions&) = delete;
  CudfOptions& operator=(const CudfOptions&) = delete;
  std::string prefix_;
};

/// Registers adapter to add cuDF operators to Drivers.
void registerCudf(const CudfOptions& options = CudfOptions::getInstance());
void unregisterCudf();

/// Returns true if cuDF is registered.
bool cudfIsRegistered();

/**
 * @brief Returns true if the velox_cudf_debug flag is set to true.
 */
bool cudfDebugEnabled();

} // namespace facebook::velox::cudf_velox
