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
#include "glog/logging.h"
#include "velox/common/base/Exceptions.h"
#include "velox/experimental/codegen/compiler_utils/CompilerOptions.h"
#include "velox/experimental/codegen/external_process/Command.h"
#include "velox/experimental/codegen/external_process/subprocess.h"
#include "velox/experimental/codegen/utils/timer/NestedScopedTimer.h"

namespace facebook::velox::codegen::compiler_utils {

/// Manage compilation of C++ code

class Compiler {
  CompilerOptions compilerOptions_;
  std::vector<std::string> defaultCompilationArgs_;
  std::vector<std::string> defaultLinkingArgs_;
  filesystem::PathGenerator pathGenerator_;
  DefaultScopedTimer::EventSequence& eventSequence_;

 public:
  explicit Compiler(
      const CompilerOptions& opt,
      DefaultScopedTimer::EventSequence& eventSequence)
      : compilerOptions_(opt), eventSequence_(eventSequence) {
    defaultCompilationArgs_.insert(
        defaultCompilationArgs_.end(),
        compilerOptions_.extraCompileOptions.begin(),
        compilerOptions_.extraCompileOptions.end());
    defaultCompilationArgs_.push_back(compilerOptions_.optimizationLevel);

    defaultLinkingArgs_.insert(
        defaultLinkingArgs_.end(),
        compilerOptions_.extraLinkOptions.begin(),
        compilerOptions_.extraLinkOptions.end());
    defaultLinkingArgs_.push_back(
        compilerOptions_
            .optimizationLevel); // Important when we start doing lto
  }

  explicit Compiler(DefaultScopedTimer::EventSequence& eventSequence)
      : compilerOptions_(), eventSequence_(eventSequence) {
    defaultCompilationArgs_.insert(
        defaultCompilationArgs_.end(),
        compilerOptions_.extraCompileOptions.begin(),
        compilerOptions_.extraCompileOptions.end());
    defaultCompilationArgs_.push_back(compilerOptions_.optimizationLevel);

    defaultLinkingArgs_.insert(
        defaultLinkingArgs_.end(),
        compilerOptions_.extraLinkOptions.begin(),
        compilerOptions_.extraLinkOptions.end());
    defaultLinkingArgs_.push_back(
        compilerOptions_
            .optimizationLevel); // Important when we start doing lto
  }

  CompilerOptions& compilerOptions() {
    return compilerOptions_;
  }

  std::vector<std::string>& defaultCompilationArgs() {
    return defaultCompilationArgs_;
  }

  std::vector<std::string>& defaultLinkingArgs() {
    return defaultLinkingArgs_;
  }

  /// Format in place a given cpp file
  /// \param path
  /// \return true iff the file was formatted
  bool formatFile(const std::filesystem::path& path) {
    if (!compilerOptions_.formatterPath) {
      LOG(INFO) << "Missing format tool path" << std::endl;
      return false;
    };
    auto format = formatCommand(path);
    std::filesystem::path err, out;
    auto proc = launchCommand(format, out, err);
    proc.wait();
    return true;
  }

  /// compiles a given c++ string
  /// \param additionalLibraries
  /// \param cppContent  c++ file content
  /// \return path to the generated object file
  std::filesystem::path compileString(
      const std::vector<LibraryDescriptor>& additionalLibraries,
      const std::string& cppContent) {
    DefaultScopedTimer timer("CompileString", eventSequence_);

    auto cppPath = pathGenerator_.tempPath("source", ".cpp");
    auto objectPath = pathGenerator_.tempPath("source", ".o");
    std::ofstream f(cppPath);
    f << cppContent;
    f.close();
    formatFile(cppPath);
    compile(additionalLibraries, cppPath, objectPath);
    return objectPath;
  }

  /// Compile a file path
  /// \param additionalLibraries
  /// \param cppFile file path
  /// \param outputFile path to the generated file
  void compile(
      const std::vector<LibraryDescriptor>& additionalLibraries,
      const std::filesystem::path& cppFile,
      const std::filesystem::path& outputFile) {
    DefaultScopedTimer timer("Compile", eventSequence_);
    auto command = compileCommand(additionalLibraries, cppFile, outputFile);
    std::filesystem::path err, out;
    auto proc = launchCommand(command, out, err);
    proc.wait();
    assert(!proc.running());
    if (proc.exit_code() != 0) {
      google::FlushLogFiles(google::INFO);
      std::cerr << "Compilation failed, Process " << proc.id() << std::endl
                << "====cerr content ====" << std::endl
                << std::ifstream(err).rdbuf() << std::endl
                << "====cout content ====" << std::endl
                << std::ifstream(out).rdbuf() << std::endl;
      throw std::logic_error(fmt::format(
          "Compilation pid {} failed, err {} , out {}",
          proc.id(),
          err.string(),
          out.string()));
    };
    return;
  }

  /// Link given object files into a final dynamic library
  /// \param additionalLibraries
  /// \param binaryFiles list of object to link
  /// \param outputFile path the resulting .so
  void link(
      const std::vector<LibraryDescriptor>& additionalLibraries,
      const std::vector<std::filesystem::path>& binaryFiles,
      const std::filesystem::path& outputFile) {
    DefaultScopedTimer timer("Link", eventSequence_);

    auto command = linkCommand(additionalLibraries, binaryFiles, outputFile);
    std::filesystem::path err, out;
    auto proc = launchCommand(command, out, err);
    proc.wait();
    if (proc.exit_code() != 0) {
      // TODO: Move printing err files in external_process
      std::cerr << "Linking failed, pid" << proc.id() << std::endl
                << "====cerr content ====" << std::endl
                << std::ifstream(err).rdbuf() << std::endl
                << "====cout content ====" << std::endl
                << std::ifstream(out).rdbuf() << std::endl;
      throw std::logic_error(fmt::format(
          "Linking pid {} failed, err {} , out {}",
          proc.id(),
          err.string(),
          out.string()));
      // throw std::logic_error(fmt::format("Linking pid {} failed",
      // proc.id()));
    }
    return;
  }

  /// link multiple binary files
  /// \param additionalLibraries
  /// \param binaryFiles
  /// \return path the generated .so
  std::filesystem::path link(
      const std::vector<LibraryDescriptor>& additionalLibraries,
      const std::vector<std::filesystem::path>& binaryFiles) {
    auto dynamicLibPath = pathGenerator_.tempPath("dyn", ".so");
    link(additionalLibraries, binaryFiles, dynamicLibPath);
    return dynamicLibPath;
  }

  /// Construct a command object which execution would compile the give files.
  /// \param additionalLibraries
  /// \param cppFile
  /// \param outputFile
  /// \return Command object for later execution.
  external_process::Command compileCommand(
      const std::vector<LibraryDescriptor>& additionalLibraries,
      const std::filesystem::path& cppFile,
      const std::filesystem::path& outputFile) {
    std::vector<std::string> args(defaultCompilationArgs_);

    for (const auto& library : compilerOptions_.defaultLibraries) {
      includePathArgs(library, args);
    };

    for (const auto& library : additionalLibraries) {
      includePathArgs(library, args);
    };

    args.push_back("-c");
    args.push_back(cppFile.string());
    args.push_back("-o");
    args.push_back(outputFile.string());

    return {compilerOptions_.compilerPath, args};
  }

  /// Linking command.
  /// \param additionalLibraries
  /// \param objectFiles
  /// \param outputFile
  /// \return Command object for later execution
  external_process::Command linkCommand(
      const std::vector<LibraryDescriptor>& additionalLibraries,
      const std::vector<std::filesystem::path>& objectFiles,
      const std::filesystem::path& outputFile) {
    std::vector<std::string> args(defaultLinkingArgs_);

    if (compilerOptions_.linker.has_value()) {
      args.push_back(
          fmt::format("-fuse-ld={}", compilerOptions_.linker.value().string()));
    }
    args.push_back("-shared");

    args.push_back("-o");
    args.push_back(outputFile.string());

    for (const auto& objectFile : objectFiles) {
      args.push_back(objectFile.string());
    }

    for (const auto& library : compilerOptions_.defaultLibraries) {
      linkingSearchPathArgs(library, args);
      linkingLibrariesArgs(library, args);
    };

    for (const auto& library : additionalLibraries) {
      linkingSearchPathArgs(library, args);
      linkingLibrariesArgs(library, args);
    };

    return {compilerOptions_.compilerPath, args};
  }

  /// In place formatting of a given file using clang-format
  /// \param cppFile file to  be formatted
  /// \return excutable command
  external_process::Command formatCommand(
      const std::filesystem::path& cppFile) {
    VELOX_CHECK(compilerOptions_.formatterPath);
    std::vector<std::string> args;
    args.push_back("-i");
    args.push_back(cppFile.string());
    args.push_back("--style=llvm");
    return {*compilerOptions_.formatterPath, args};
  }

 private:
  void includePathArgs(
      const LibraryDescriptor& library,
      std::vector<std::string>& args) {
    for (const auto& path : library.systemIncludePath) {
      args.push_back("-isystem");
      args.push_back(path.string());
    }
    for (const auto& path : library.includePath) {
      args.push_back("-I");
      args.push_back(path.string());
    }
  }

  void linkingSearchPathArgs(
      const LibraryDescriptor& library,
      std::vector<std::string>& args) {
    for (const auto& path : library.linkerSearchPath) {
      args.push_back("-L");
      args.push_back(path.string());
    }
  }

  void linkingLibrariesArgs(
      const LibraryDescriptor& library,
      std::vector<std::string>& args) {
    for (const auto& name : library.libraryNames) {
      args.push_back("-l");
      args.push_back(name);
    };
    for (const auto& arg : library.additionalLinkerObject) {
      args.push_back(arg.string());
    };
  }
};
} // namespace facebook::velox::codegen::compiler_utils
