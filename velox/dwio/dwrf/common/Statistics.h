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

#include "velox/dwio/common/Statistics.h"
#include "velox/dwio/dwrf/common/Common.h"
#include "velox/dwio/dwrf/common/wrap/dwrf-proto-wrapper.h"

namespace facebook::velox::dwrf {

/**
 * StatsContext contains fields required to compute statistics
 */
struct StatsContext : public dwio::common::StatsContext {
  const std::string writerName;
  WriterVersion writerVersion;

  StatsContext(const std::string& name, WriterVersion version)
      : writerName(name), writerVersion{version} {}

  explicit StatsContext(WriterVersion version)
      : writerName(""), writerVersion{version} {}

  ~StatsContext() override = default;
};

std::unique_ptr<dwio::common::ColumnStatistics> buildColumnStatisticsFromProto(
    const proto::ColumnStatistics& stats,
    const StatsContext& statsContext);

} // namespace facebook::velox::dwrf
