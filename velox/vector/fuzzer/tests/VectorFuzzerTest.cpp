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

#include "velox/common/memory/Memory.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"

using namespace facebook::velox;

namespace {

class VectorFuzzerTest : public testing::Test {
 public:
  memory::MemoryPool* pool() const {
    return pool_.get();
  }

 private:
  std::unique_ptr<memory::MemoryPool> pool_{
      memory::getDefaultScopedMemoryPool()};
};

// TODO: add coverage for other VectorFuzzer methods.

TEST_F(VectorFuzzerTest, constants) {
  VectorFuzzer::Options opts;
  opts.nullRatio = 0;
  VectorFuzzer fuzzer(opts, pool());

  // Non-null primitive constants.
  auto vector = fuzzer.fuzzConstant(INTEGER());
  ASSERT_TRUE(vector->type()->kindEquals(INTEGER()));
  ASSERT_EQ(VectorEncoding::Simple::CONSTANT, vector->encoding());
  ASSERT_FALSE(vector->mayHaveNulls());

  vector = fuzzer.fuzzConstant(VARCHAR());
  ASSERT_TRUE(vector->type()->kindEquals(VARCHAR()));
  ASSERT_EQ(VectorEncoding::Simple::CONSTANT, vector->encoding());
  ASSERT_FALSE(vector->mayHaveNulls());

  // Non-null complex types.
  vector = fuzzer.fuzzConstant(MAP(BIGINT(), SMALLINT()));
  ASSERT_TRUE(vector->type()->kindEquals(MAP(BIGINT(), SMALLINT())));
  ASSERT_EQ(VectorEncoding::Simple::CONSTANT, vector->encoding());
  ASSERT_FALSE(vector->mayHaveNulls());

  vector = fuzzer.fuzzConstant(ROW({ARRAY(BIGINT()), SMALLINT()}));
  ASSERT_TRUE(vector->type()->kindEquals(ROW({ARRAY(BIGINT()), SMALLINT()})));
  ASSERT_EQ(VectorEncoding::Simple::CONSTANT, vector->encoding());
  ASSERT_FALSE(vector->mayHaveNulls());
}

TEST_F(VectorFuzzerTest, constantsNull) {
  VectorFuzzer::Options opts;
  opts.nullRatio = 1; // 1 = 100%
  VectorFuzzer fuzzer(opts, pool());

  // Null constants.
  auto vector = fuzzer.fuzzConstant(REAL());
  ASSERT_TRUE(vector->type()->kindEquals(REAL()));
  ASSERT_EQ(VectorEncoding::Simple::CONSTANT, vector->encoding());
  ASSERT_TRUE(vector->mayHaveNulls());

  // Null complex types.
  vector = fuzzer.fuzzConstant(ARRAY(VARCHAR()));
  ASSERT_TRUE(vector->type()->kindEquals(ARRAY(VARCHAR())));
  ASSERT_EQ(VectorEncoding::Simple::CONSTANT, vector->encoding());
  ASSERT_TRUE(vector->mayHaveNulls());
}

} // namespace
