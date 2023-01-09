/*
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
#include "presto_cpp/main/QueryContextManager.h"

using namespace facebook::velox;
using namespace facebook::presto;

namespace {
void verifyQueryCtxCache(
    QueryContextCache& cache,
    std::unordered_map<protocol::QueryId, std::shared_ptr<core::QueryCtx>>&
        queryCtxs,
    int startQueryIdx,
    int numQueries) {
  for (int i = startQueryIdx; i < startQueryIdx + numQueries; ++i) {
    auto queryId = fmt::format("query-{}", i);
    EXPECT_EQ(queryCtxs[queryId].get(), cache.get(queryId).get());
  }
}
} // namespace

TEST(QueryContextCacheTest, basic) {
  QueryContextCache queryContextCache;

  // Insert 16 query contexts.
  std::unordered_map<protocol::QueryId, std::shared_ptr<core::QueryCtx>>
      queryCtxs;

  for (int i = 0; i < 16; ++i) {
    auto queryId = fmt::format("query-{}", i);
    auto queryCtx = std::make_shared<core::QueryCtx>();
    queryCtxs[queryId] = queryCtx;
    queryContextCache.insert(queryId, queryCtx);
  }

  EXPECT_EQ(queryContextCache.size(), 16);

  // Verify that cache returns the same query context for the same queryId.
  verifyQueryCtxCache(queryContextCache, queryCtxs, 0, 16);

  EXPECT_EQ(queryContextCache.size(), 16);

  // Remove strong references to query contexts.
  queryCtxs.clear();

  // Verify that cache returns no query context now.
  verifyQueryCtxCache(queryContextCache, queryCtxs, 0, 16);
  EXPECT_EQ(queryContextCache.size(), 0);
}

TEST(QueryContextCacheTest, eviction) {
  QueryContextCache queryContextCache(8);

  // Insert 8 query contexts.
  std::unordered_map<protocol::QueryId, std::shared_ptr<core::QueryCtx>>
      queryCtxs;

  for (int i = 0; i < 8; ++i) {
    auto queryId = fmt::format("query-{}", i);
    auto queryCtx = std::make_shared<core::QueryCtx>();
    queryCtxs[queryId] = queryCtx;
    queryContextCache.insert(queryId, queryCtx);
  }
  EXPECT_EQ(queryContextCache.size(), 8);

  // Verify that cache returns the same query context for the same queryId.
  verifyQueryCtxCache(queryContextCache, queryCtxs, 0, 8);
  EXPECT_EQ(queryContextCache.size(), 8);

  // Release query-0 to query-3;
  for (int i = 0; i < 4; ++i) {
    auto queryId = fmt::format("query-{}", i);
    queryCtxs.erase(queryId);
  }

  // Insert 4 more query ctxs
  for (int i = 8; i < 12; ++i) {
    auto queryId = fmt::format("query-{}", i);
    auto queryCtx = std::make_shared<core::QueryCtx>();
    queryCtxs[queryId] = queryCtx;
    queryContextCache.insert(queryId, queryCtx);
  }

  EXPECT_EQ(queryContextCache.size(), 8);

  verifyQueryCtxCache(queryContextCache, queryCtxs, 0, 12);
  EXPECT_EQ(queryContextCache.size(), 8);

  // Ensure that cache expands if all the queries in cache are alive.
  for (int i = 12; i < 20; ++i) {
    auto queryId = fmt::format("query-{}", i);
    auto queryCtx = std::make_shared<core::QueryCtx>();
    queryCtxs[queryId] = queryCtx;
    queryContextCache.insert(queryId, queryCtx);
  }
  EXPECT_EQ(queryContextCache.size(), 16);

  verifyQueryCtxCache(queryContextCache, queryCtxs, 0, 20);

  EXPECT_EQ(queryContextCache.size(), 16);

  queryCtxs.clear();

  // Verify that cache returns no query context now.
  verifyQueryCtxCache(queryContextCache, queryCtxs, 0, 20);
  EXPECT_EQ(queryContextCache.size(), 0);
}
