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

#include <cstdint>
#include <cstring>
#include <string>

#include "velox/common/base/Exceptions.h"

namespace facebook::velox::dwrf {

// ORC/DWRF file ends with: [PostScript protobuf][1 byte postscript length].
// Call before PostScript/Footer ParseFromArray so failures are not opaque
// protobuf errors (short HDFS/CachedInput reads, truncated files, bad magic).
inline void validatePostScriptBuffer(
    const void* data,
    size_t bufferSize,
    size_t postScriptLen,
    uint64_t fileSize,
    uint64_t bytesRequested,
    uint64_t bytesReturned,
    uint64_t expectedFileSize = 0) {
  VELOX_CHECK_NOT_NULL(data, "Footer buffer is null, fileSize={}", fileSize);

  VELOX_CHECK_GT(fileSize, 0, "ORC/DWRF file is empty");

  // BufferedInput/HDFS short-read: requested last N bytes, got fewer.
  VELOX_CHECK_EQ(
      bytesRequested,
      bytesReturned,
      "Short read of ORC/DWRF footer: requested {} bytes from end of file "
      "(fileSize={}) but BufferedInput returned {} bytes. Not a protobuf error.",
      bytesRequested,
      fileSize,
      bytesReturned);

  if (expectedFileSize > 0) {
    VELOX_CHECK_EQ(
        fileSize,
        expectedFileSize,
        "ORC/DWRF file size mismatch vs split: actual={}, expected={}. "
        "Possible truncated object or wrong split range.",
        fileSize,
        expectedFileSize);
  }

  VELOX_CHECK_GE(
      bufferSize,
      postScriptLen + 1,
      "ORC/DWRF buffer too small for postscript: bufferSize={}, "
      "postScriptLen={}, fileSize={}. File end is truncated or postscript "
      "length byte is corrupt.",
      bufferSize,
      postScriptLen,
      fileSize);

  VELOX_CHECK_GT(
      postScriptLen,
      0,
      "Invalid ORC/DWRF postscript length 0 (fileSize={}, bufferSize={})",
      fileSize,
      bufferSize);

  VELOX_CHECK_LE(
      postScriptLen + 1,
      fileSize,
      "Postscript length {} (+1 length byte) exceeds file size {}",
      postScriptLen,
      fileSize);
}

// Optional: when the leading magic is available in the same read window
// (small files), verify 'ORC'. Large files keep magic only at offset 0.
inline void validateOrcMagicIfPresent(
    const char* fileStart,
    size_t available,
    uint64_t fileSize) {
  static constexpr char kOrcMagic[] = {'O', 'R', 'C'};
  if (fileStart == nullptr || available < sizeof(kOrcMagic)) {
    return;
  }
  if (std::memcmp(fileStart, kOrcMagic, sizeof(kOrcMagic)) != 0) {
    VELOX_FAIL(
        "Invalid ORC/DWRF magic at file start (fileSize={}): expected 'ORC'",
        fileSize);
  }
}

inline void checkProtoParse(
    bool ok,
    const char* section,
    size_t sectionLen,
    uint64_t fileSize,
    size_t postScriptLen) {
  VELOX_CHECK(
      ok,
      "Failed to ParseFromArray {} (sectionLen={}, fileSize={}, "
      "postScriptLen={}). Bytes at file end are not valid protobuf. "
      "Causes: short HDFS/CachedInput read, truncated file, unsupported "
      "ORC version/encryption/writer, or corrupt footer. Compare with "
      "orc-tools meta / presto-orc.",
      section,
      sectionLen,
      fileSize,
      postScriptLen);
}

} // namespace facebook::velox::dwrf
