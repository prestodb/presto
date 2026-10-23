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

#include "velox/dwio/dwrf/reader/ReaderBase.h"
#include "velox/dwio/dwrf/reader/FooterParseValidation.h"

// NOTE: This file is a surgical overlay for footer/postscript load only.
// Keep all other ReaderBase methods in the tree unchanged; if your tree
// already defines loadPostScript/loadFooter below, replace those bodies
// with the validated versions here and drop any duplicate definitions.

namespace facebook::velox::dwrf {
namespace {

// Consistent with ORC Java/C++ writers: postscript fits in this window.
constexpr uint64_t kPostScriptReadGuess = 256 * 1024;

} // namespace

// Stricter postscript load: length checks, short-read detection, clear errors.
// Intended body for ReaderBase postscript path (ParseFromArray site).
template <typename PostScriptProto, typename Input, typename Pool>
PostScriptProto loadPostScriptChecked(
    Input& input,
    Pool& pool,
    uint64_t fileSize,
    uint64_t expectedFileSize = 0) {
  VELOX_CHECK_GT(fileSize, 0, "ORC/DWRF file is empty");

  const uint64_t readSize = std::min(fileSize, kPostScriptReadGuess);
  auto buffer = input.read(fileSize - readSize, readSize);
  const char* data = buffer->template as<char>();
  const uint64_t bytesReturned = buffer->size();

  // Last byte is postscript length.
  VELOX_CHECK_GE(bytesReturned, 1, "Empty footer read, fileSize={}", fileSize);
  const size_t psLen = static_cast<uint8_t>(data[bytesReturned - 1]);

  validatePostScriptBuffer(
      data,
      bytesReturned,
      psLen,
      fileSize,
      readSize,
      bytesReturned,
      expectedFileSize);

  // Whole-file window: magic at offset 0 is inside the buffer.
  if (readSize == fileSize) {
    validateOrcMagicIfPresent(data, bytesReturned, fileSize);
  }

  PostScriptProto ps;
  const bool ok =
      ps.ParseFromArray(data + bytesReturned - 1 - psLen, static_cast<int>(psLen));
  checkProtoParse(ok, "postscript", psLen, fileSize, psLen);
  return ps;
}

template <typename FooterProto>
void loadFooterChecked(
    FooterProto& footer,
    const void* data,
    size_t footerLen,
    uint64_t fileSize,
    size_t postScriptLen) {
  VELOX_CHECK_NOT_NULL(data, "Footer data null, fileSize={}", fileSize);
  VELOX_CHECK_GT(
      footerLen,
      0,
      "ORC/DWRF footer length is 0 (fileSize={}, postScriptLen={})",
      fileSize,
      postScriptLen);
  const bool ok =
      footer.ParseFromArray(data, static_cast<int>(footerLen));
  checkProtoParse(ok, "footer", footerLen, fileSize, postScriptLen);
}

} // namespace facebook::velox::dwrf
