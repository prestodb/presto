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

#include "velox/connectors/hive/HiveConfig.h"

#include <boost/algorithm/string.hpp>

namespace facebook::velox::connector::hive {

namespace {

HiveConfig::InsertExistingPartitionsBehavior
stringToInsertExistingPartitionsBehavior(const std::string& strValue) {
  auto upperValue = boost::algorithm::to_upper_copy(strValue);
  if (upperValue == "ERROR") {
    return HiveConfig::InsertExistingPartitionsBehavior::kError;
  }
  if (upperValue == "OVERWRITE") {
    return HiveConfig::InsertExistingPartitionsBehavior::kOverwrite;
  }
  VELOX_UNSUPPORTED(
      "Unsupported insert existing partitions behavior: {}.", strValue);
}

} // namespace

// static
HiveConfig::InsertExistingPartitionsBehavior
HiveConfig::insertExistingPartitionsBehavior(const Config* config) {
  auto strBehavior =
      config->get<std::string>(kInsertExistingPartitionsBehavior);
  return strBehavior.has_value()
      ? stringToInsertExistingPartitionsBehavior(strBehavior.value())
      : InsertExistingPartitionsBehavior::kError;
}

// static
uint32_t HiveConfig::maxPartitionsPerWriters(const Config* config) {
  return config->get<uint32_t>(kMaxPartitionsPerWriters, 100);
}

} // namespace facebook::velox::connector::hive
