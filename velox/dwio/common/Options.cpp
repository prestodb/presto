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

#include "velox/dwio/common/Options.h"

namespace facebook {
namespace dwio {
namespace common {

FileFormat ToFileFormat(std::string s) {
  if (s == "orc") {
    return FileFormat::ORC;
  } else if (s == "rc") {
    return FileFormat::RC;
  } else if (s == "rc:text") {
    return FileFormat::RC_TEXT;
  } else if (s == "rc:binary") {
    return FileFormat::RC_BINARY;
  } else if (s == "text") {
    return FileFormat::TEXT;
  } else if (s == "json") {
    return FileFormat::JSON;
  }
  return FileFormat::UNKNOWN;
}

std::string ToString(FileFormat fmt) {
  switch (fmt) {
    case FileFormat::ORC:
      return "orc";
    case FileFormat::RC:
      return "rc";
    case FileFormat::RC_TEXT:
      return "rc:text";
    case FileFormat::RC_BINARY:
      return "rc:binary";
    case FileFormat::TEXT:
      return "text";
    case FileFormat::JSON:
      return "json";
    default:
      return "unknown";
  }
}

} // namespace common
} // namespace dwio
} // namespace facebook
