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

#include <glog/logging.h>
#include <filesystem>
#include <string>
#include <vector>
#include "boost/process.hpp"
#include "boost/process/extend.hpp"
#include "velox/experimental/codegen/external_process/Command.h"
#include "velox/experimental/codegen/external_process/ExternalProcessException.h"
#include "velox/experimental/codegen/external_process/Filesystem.h"
namespace facebook::velox::codegen::external_process {

namespace bp = boost::process;

static bool logExecuteCommand = true;

__attribute__((weak)) inline auto launchCommand(
    const Command& command,
    std::filesystem::path& out,
    std::filesystem::path& err) {
  struct processInfo {
    pid_t pid;
    std::chrono::time_point<std::chrono::steady_clock> startTime;
  };

  auto fileStatus = std::filesystem::status(command.executablePath);
  if (!std::filesystem::exists(fileStatus) or
      ((fileStatus.permissions() &
        (std::filesystem::perms::others_exec |
         std::filesystem::perms::group_exec |
         std::filesystem::perms::owner_exec)) ==
       std::filesystem::perms::none)) {
    throw ExternalProcessException(
        fmt::format("{} is not executable", command.executablePath.string()));
  };

  compiler_utils::filesystem::PathGenerator pathGen;
  out = pathGen.tempPath("stdout", ".txt");
  err = pathGen.tempPath("stderr", ".txt");

  // Communication channel between call backs;
  // Ideally we would want to just use a promise to represent the communication
  // channel, However, since we need the call backs lambda to be
  // copy-constructable, we can't directly capture a std::promise/std::future
  // because those classes are not copy constructable.
  auto channel = std::make_shared<std::promise<processInfo>>();

  auto on_exit = [channel, command = command, out, err](
                     int exit_code, const std::error_code& code) mutable {
    auto processInfo = channel.get()->get_future().get();
    LOG_IF(INFO, logExecuteCommand)
        << "Process completed, Pid : " << processInfo.pid
        << " outLog : " << out.string() << ", errLog : " << err.string()
        << ", command : " << command.toString(" ");
  };

  auto on_success = [channel, command, out, err](auto& executor) {
    LOG_IF(INFO, logExecuteCommand)
        << "Process started, Pid : " << executor.pid
        << ", outLog : " << out.string() << ", errLog : " << err.string();
    LOG_IF(INFO, logExecuteCommand)
        << "Command for Pid " << executor.pid << " : " << command.toString(" ")
        << std::endl;
    channel.get()->set_value({executor.pid, std::chrono::steady_clock::now()});
  };

  try {
    // TODO: the on_exit handler is not triggered
    bp::child child(
        command.executablePath.string(),
        command.arguments,
        bp::std_out > out,
        bp::std_err > err,
        bp::on_exit = on_exit,
        bp::extend::on_success = on_success);
    return child;
  } catch (const std::system_error& e) {
    // TODO: Normally, we would want to handle this in the on_error callback.
    // Howevere, the on_error seems to not work properly on macos.
    LOG(INFO) << "Process creation failed : " << command.executablePath.string()
              << std::endl;
    throw;
  }
}

} // namespace facebook::velox::codegen::external_process
