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

#include "velox/substrait/tests/JsonToProtoConverter.h"
#include <fstream>
#include <sstream>
#include "velox/common/base/Exceptions.h"

void JsonToProtoConverter::readFromFile(
    const std::string& msgPath,
    google::protobuf::Message& msg) {
  // Read json file and resume the Substrait plan.
  std::ifstream msgJson(msgPath);
  VELOX_CHECK(
      !msgJson.fail(), "Failed to open file: {}. {}", msgPath, strerror(errno));
  std::stringstream buffer;
  buffer << msgJson.rdbuf();
  std::string msgData = buffer.str();
  auto status = google::protobuf::util::JsonStringToMessage(msgData, &msg);
  VELOX_CHECK(
      status.ok(),
      "Failed to parse Substrait JSON: {} {}",
      status.code(),
      status.message());
}
