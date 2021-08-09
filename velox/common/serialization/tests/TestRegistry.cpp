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
#include "velox/common/base/Exceptions.h"
#include "velox/common/serialization/Registry.h"

using namespace ::facebook::velox;

namespace {
TEST(Registry, SmartPointerFactoryWithNoArgument) {
  Registry<size_t, std::unique_ptr<size_t>()> registry;

  const size_t key = 0;
  const size_t value = 1;

  EXPECT_FALSE(registry.Has(key));
  EXPECT_EQ(registry.Create(key), nullptr);

  registry.Register(0, [value]() -> std::unique_ptr<size_t> {
    return std::make_unique<size_t>(value);
  });

  EXPECT_TRUE(registry.Has(key));
  EXPECT_EQ(*registry.Create(key), value);
}

TEST(Registry, ValueFactoryWithArguments) {
  Registry<size_t, size_t(size_t, size_t)> registry;

  const size_t key = 0;

  EXPECT_FALSE(registry.Has(key));
  EXPECT_THROW(registry.Create(key, 1, 1), facebook::velox::VeloxUserError);

  registry.Register(0, [](size_t l, size_t r) -> size_t { return l + r; });

  EXPECT_TRUE(registry.Has(key));
  EXPECT_EQ(registry.Create(key, 1, 1), 2);
}
} // namespace
