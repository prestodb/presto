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

#include "velox/common/serialization/Serializable.h"
#include <gtest/gtest.h>
#include "folly/container/F14Map.h"
#include "folly/json.h"

using namespace ::facebook::velox;

namespace {
TEST(SerializableTest, options) {
  auto opts = getSerializationOptions();

  // A very large negative number that is out of folly Json integer bound.
  // With opts.double_fallback enabled, it should be correctly handled as double
  // type.
  auto largeNumber = folly::parseJson("-21820650861507478000", opts);
  EXPECT_TRUE(largeNumber.isDouble());
  EXPECT_EQ(largeNumber, -21820650861507478000.0);
}

class Dot : public ISerializable {
 public:
  Dot(std::string color, int32_t radius)
      : color_{std::move(color)}, radius_{radius} {}

  const std::string& color() const {
    return color_;
  }

  int32_t radius() const {
    return radius_;
  }

  bool operator==(const Dot& other) const {
    return color_ == other.color() && radius_ == other.radius();
  }

  Dot operator*(int32_t n) const {
    return Dot(color_, radius_ * n);
  }

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object();

    obj["name"] = "Dot";
    obj["color"] = color_;
    obj["radius"] = radius_;
    return obj;
  }

  static std::shared_ptr<Dot> create(const folly::dynamic& obj, void* context) {
    int32_t multiplier = 1;
    if (context) {
      multiplier = *static_cast<int32_t*>(context);
    }

    return std::make_shared<Dot>(
        obj["color"].asString(), obj["radius"].asInt() * multiplier);
  }

 private:
  std::string color_;
  int32_t radius_;
};

TEST(SerializableTest, basic) {
  DeserializationWithContextRegistryForSharedPtr().Register("Dot", Dot::create);

  Dot dot{"yellow", 10};
  auto serialized = dot.serialize();

  auto copy = ISerializable::deserialize<Dot>(serialized);
  ASSERT_EQ(*copy, dot);

  std::vector<Dot> dots = {Dot("blue", 1), Dot("red", 2)};
  serialized = ISerializable::serialize(dots);

  {
    auto copies = ISerializable::deserialize<std::vector<Dot>>(serialized);
    ASSERT_EQ(copies.size(), dots.size());
    for (auto i = 0; i < dots.size(); ++i) {
      ASSERT_EQ(*copies[i], dots[i]);
    }
  }

  std::map<int32_t, Dot> dotsByRadius;
  for (const auto& dot : dots) {
    dotsByRadius.emplace(dot.radius(), dot);
  }
  serialized = ISerializable::serialize(dotsByRadius);

  {
    auto copies =
        ISerializable::deserialize<std::map<int32_t, Dot>>(serialized);
    ASSERT_EQ(copies.size(), dotsByRadius.size());
    for (const auto& [radius, dot] : dotsByRadius) {
      ASSERT_EQ(*copies.at(radius), dot);
    }
  }
}

TEST(SerializableTest, context) {
  DeserializationWithContextRegistryForSharedPtr().Register("Dot", Dot::create);

  Dot dot{"yellow", 10};
  auto serialized = dot.serialize();

  // Deserialize with different multipliers for radius passed via 'context'.
  int32_t multiplier = 1;
  auto copy = ISerializable::deserialize<Dot>(serialized, &multiplier);
  ASSERT_EQ(*copy, dot);

  multiplier = 10;
  copy = ISerializable::deserialize<Dot>(serialized, &multiplier);
  ASSERT_EQ(*copy, dot * 10);

  // A list of dots.
  std::vector<Dot> dots = {Dot("blue", 1), Dot("red", 2)};
  serialized = ISerializable::serialize(dots);

  {
    auto copies =
        ISerializable::deserialize<std::vector<Dot>>(serialized, &multiplier);
    ASSERT_EQ(copies.size(), dots.size());
    for (auto i = 0; i < dots.size(); ++i) {
      ASSERT_EQ(*copies[i], dots[i] * 10);
    }
  }

  // A map of dots.
  std::map<int32_t, Dot> dotsByRadius;
  for (const auto& dot : dots) {
    dotsByRadius.emplace(dot.radius(), dot);
  }
  serialized = ISerializable::serialize(dotsByRadius);

  {
    auto copies = ISerializable::deserialize<std::map<int32_t, Dot>>(
        serialized, &multiplier);
    ASSERT_EQ(copies.size(), dotsByRadius.size());
    for (const auto& [radius, dot] : dotsByRadius) {
      ASSERT_EQ(*copies.at(radius), dot * 10);
    }
  }
}

template <
    template <typename, typename, typename...>
    typename TMap,
    typename TKey,
    typename TMapped,
    typename TIt,
    typename... TArgs>
void testMap(TIt first, TIt last) {
  TMap<TKey, TMapped, TArgs...> map{first, last};
  auto serialized = ISerializable::serialize(map);
  auto copy = ISerializable::deserialize<TMap, TKey, TMapped>(serialized);
  ASSERT_EQ(map, copy);
}

TEST(SerializableTest, map) {
  std::vector<std::pair<int32_t, std::string>> vals{
      {1, "a"}, {2, "b"}, {3, "c"}};
  testMap<std::map, int32_t, std::string>(vals.begin(), vals.end());
  testMap<std::unordered_map, int32_t, std::string>(vals.begin(), vals.end());
  testMap<folly::F14FastMap, int32_t, std::string>(vals.begin(), vals.end());
}

} // namespace
