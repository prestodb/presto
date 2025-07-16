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

#include "velox/functions/prestosql/tests/CastBaseTest.h"
#include "velox/functions/prestosql/types/SfmSketchType.h"

using namespace facebook::velox;

class SfmsketchCastTest : public functions::test::CastBaseTest {};

TEST_F(SfmsketchCastTest, toSfmSketch) {
  testCast<StringView, StringView>(
      VARBINARY(),
      SFMSKETCH(),
      {"sfm_sketch"_sv, ""_sv, std::nullopt},
      {"sfm_sketch"_sv, ""_sv, std::nullopt});
  testCast<StringView, StringView>(
      VARBINARY(),
      SFMSKETCH(),
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}

TEST_F(SfmsketchCastTest, fromSfmSketch) {
  testCast<StringView, StringView>(
      SFMSKETCH(),
      VARBINARY(),
      {"sfm_sketch"_sv, ""_sv, std::nullopt},
      {"sfm_sketch"_sv, ""_sv, std::nullopt});
  testCast<StringView, StringView>(
      SFMSKETCH(),
      VARBINARY(),
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt},
      {std::nullopt, std::nullopt, std::nullopt, std::nullopt});
}
