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
#include <gtest/gtest.h>

#include "velox/core/Context.h"
#include "velox/core/QueryConfig.h"
#include "velox/core/QueryCtx.h"

using ::facebook::velox::core::MemConfig;
using ::facebook::velox::core::QueryConfig;
using ::facebook::velox::core::QueryCtx;

TEST(TestQueryConfig, emptyConfig) {
  std::unordered_map<std::string, std::string> configData;
  auto queryCtxConfig = std::make_shared<MemConfig>(configData);
  auto queryCtx = std::make_shared<QueryCtx>(nullptr, queryCtxConfig);
  const QueryConfig& config = queryCtx->queryConfig();

  ASSERT_FALSE(config.codegenEnabled());
  ASSERT_EQ(config.codegenConfigurationFilePath(), "");
  ASSERT_FALSE(config.isCastIntByTruncate());
}

TEST(TestQueryConfig, setConfig) {
  std::string path = "/tmp/CodeGenConfig";
  std::unordered_map<std::string, std::string> configData(
      {{QueryConfig::kCodegenEnabled, "true"},
       {QueryConfig::kCodegenConfigurationFilePath, path}});
  auto queryCtxConfig = std::make_shared<MemConfig>(configData);
  auto queryCtx = std::make_shared<QueryCtx>(nullptr, queryCtxConfig);
  const QueryConfig& config = queryCtx->queryConfig();

  ASSERT_TRUE(config.codegenEnabled());
  ASSERT_EQ(config.codegenConfigurationFilePath(), path);
  ASSERT_FALSE(config.isCastIntByTruncate());
}
