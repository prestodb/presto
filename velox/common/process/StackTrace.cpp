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

#include "velox/common/process/StackTrace.h"

// Symbolizer requires folly to be compiled with libelf and libdwarf support
// (also currently only works in Linux).
#if __linux__ && FOLLY_HAVE_ELF && FOLLY_HAVE_DWARF
#define VELOX_HAS_SYMBOLIZER 1
#else
#define VELOX_HAS_SYMBOLIZER 0
#endif

#include <algorithm>
#include <fstream>

#include <fmt/format.h>
#include <folly/Indestructible.h>
#include <folly/String.h>
#include <folly/experimental/symbolizer/StackTrace.h>

#include "velox/common/process/ProcessBase.h"

#ifdef __linux__
#include <folly/experimental/symbolizer/Symbolizer.h> // @manual
#include <folly/fibers/FiberManager.h> // @manual
#endif

namespace facebook::velox::process {

StackTrace::StackTrace(int32_t skipFrames) {
  create(skipFrames);
}

StackTrace::StackTrace(const StackTrace& other) {
  bt_pointers_ = other.bt_pointers_;
  if (folly::test_once(other.bt_vector_flag_)) {
    bt_vector_ = other.bt_vector_;
    folly::set_once(bt_vector_flag_);
  }
  if (folly::test_once(other.bt_flag_)) {
    bt_ = other.bt_;
    folly::set_once(bt_flag_);
  }
}

StackTrace& StackTrace::operator=(const StackTrace& other) {
  if (this != &other) {
    this->~StackTrace();
    new (this) StackTrace(other);
  }
  return *this;
}

void StackTrace::create(int32_t skipFrames) {
  const int32_t kDefaultSkipFrameAdjust = 2; // ::create(), ::StackTrace()
  const int32_t kMaxFrames = 75;

  bt_pointers_.clear();
  uintptr_t btpointers[kMaxFrames];
  ssize_t framecount = folly::symbolizer::getStackTrace(btpointers, kMaxFrames);
  if (framecount <= 0) {
    return;
  }

  framecount = std::min(framecount, static_cast<ssize_t>(kMaxFrames));
  skipFrames = std::max(skipFrames + kDefaultSkipFrameAdjust, 0);

  bt_pointers_.reserve(framecount - skipFrames);
  for (int32_t i = skipFrames; i < framecount; i++) {
    bt_pointers_.push_back(reinterpret_cast<void*>(btpointers[i]));
  }
}

///////////////////////////////////////////////////////////////////////////////
// reporting functions

const std::vector<std::string>& StackTrace::toStrVector() const {
  folly::call_once(bt_vector_flag_, [&] {
    size_t frame = 0;
    static folly::Indestructible<folly::fbstring> myname{
        folly::demangle(typeid(decltype(*this))) + "::"};
    bt_vector_.reserve(bt_pointers_.size());
    for (auto ptr : bt_pointers_) {
      auto framename = translateFrame(ptr);
      if (folly::StringPiece(framename).startsWith(*myname)) {
        continue; // ignore frames in the StackTrace class
      }
      bt_vector_.push_back(fmt::format("# {:<2d} {}", frame++, framename));
    }
  });
  return bt_vector_;
}

const std::string& StackTrace::toString() const {
  folly::call_once(bt_flag_, [&] {
    const auto& vec = toStrVector();
    size_t needed = 0;
    for (const auto& frame : vec) {
      needed += frame.size() + 1;
    }
    bt_.reserve(needed);
    for (const auto& frame_title : vec) {
      bt_ += frame_title;
      bt_ += '\n';
    }
  });
  return bt_;
}

std::string StackTrace::log(
    const char* errorType,
    std::string* out /* = NULL */) const {
  std::string pid = folly::to<std::string>(getProcessId());

  std::string msg;
  msg += "Host: " + getHostName();
  msg += "\nProcessID: " + pid;
  msg += "\nThreadID: " +
      folly::to<std::string>(reinterpret_cast<uintptr_t>(getThreadId()));
  msg += "\nName: " + getAppName();
  msg += "\nType: ";
  if (errorType) {
    msg += errorType;
  } else {
    msg += "(unknown error)";
  }
  msg += "\n\n";
  msg += toString();
  msg += "\n";

  std::string tracefn = "/tmp/stacktrace." + pid + ".log";
  std::ofstream f(tracefn.c_str());
  if (f) {
    f << msg;
    f.close();
  }

  if (out) {
    *out = msg;
  }
  return tracefn;
}

#if VELOX_HAS_SYMBOLIZER
namespace {
inline std::string translateFrameImpl(void* addressPtr) {
  // TODO: lineNumbers has been disabled since 2009.
  using namespace folly::symbolizer;

  std::uintptr_t address = reinterpret_cast<std::uintptr_t>(addressPtr);
  Symbolizer symbolizer(LocationInfoMode::DISABLED);
  SymbolizedFrame frame;
  symbolizer.symbolize(address, frame);

  StringSymbolizePrinter printer(SymbolizePrinter::TERSE);
  printer.print(frame);
  return printer.str();
}
} // namespace
#endif

std::string StackTrace::translateFrame(void* addressPtr, bool /*lineNumbers*/) {
#if VELOX_HAS_SYMBOLIZER
  return folly::fibers::runInMainContext(
      [addressPtr]() { return translateFrameImpl(addressPtr); });
#else
  (void)addressPtr;
  return std::string{};
#endif
}

std::string StackTrace::demangle(const char* mangled) {
  return folly::demangle(mangled).toStdString();
}

} // namespace facebook::velox::process
