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
#include "velox/type/Subfield.h"
#include <gtest/gtest.h>
#include "velox/type/Tokenizer.h"

using namespace facebook::velox::common;

std::vector<std::unique_ptr<Subfield::PathElement>> tokenize(
    const std::string& path,
    std::shared_ptr<const Separators> separators = Separators::get()) {
  std::vector<std::unique_ptr<Subfield::PathElement>> elements;
  Tokenizer tokenizer(path, std::move(separators));
  while (tokenizer.hasNext()) {
    elements.push_back(tokenizer.next());
  }
  return elements;
}

void assertInvalidSubfield(
    const std::string& subfield,
    const std::string& message) {
  try {
    tokenize(subfield);
    ASSERT_TRUE(false) << "Expected an exception parsing " << subfield;
  } catch (facebook::velox::VeloxRuntimeError& e) {
    ASSERT_EQ(e.message(), message);
  }
}

TEST(SubfieldTest, invalidPaths) {
  assertInvalidSubfield("a[b]", "Invalid index b]");
  assertInvalidSubfield("a[2", "Invalid subfield path: a[2^");
  assertInvalidSubfield("a.*", "Invalid subfield path: a.^*");
  assertInvalidSubfield("a[2].[3].", "Invalid subfield path: a[2].^[3].");
}

void testColumnName(
    const std::string& name,
    std::shared_ptr<const Separators> separators = Separators::get()) {
  auto elements = tokenize(name, std::move(separators));
  EXPECT_EQ(elements.size(), 1);
  EXPECT_EQ(*elements[0].get(), Subfield::NestedField(name));
}

TEST(SubfieldTest, columnNamesWithSpecialCharacters) {
  testColumnName("two words");
  testColumnName("two  words");
  testColumnName("one two three");
  testColumnName("$bucket");
  testColumnName("apollo-11");
  testColumnName("a/b/c:12");
  testColumnName("@basis");
  testColumnName("@basis|city_id");
  auto separators = std::make_shared<Separators>();
  separators->dot = '\0';
  testColumnName("city.id@address:number/date|day$a-b$10_bucket", separators);
}

std::vector<std::unique_ptr<Subfield::PathElement>> createElements() {
  std::vector<std::unique_ptr<Subfield::PathElement>> elements;
  elements.push_back(std::make_unique<Subfield::NestedField>("b"));
  elements.push_back(std::make_unique<Subfield::LongSubscript>(2));
  elements.push_back(std::make_unique<Subfield::LongSubscript>(-1));
  elements.push_back(std::make_unique<Subfield::StringSubscript>("z"));
  elements.push_back(std::make_unique<Subfield::AllSubscripts>());
  elements.push_back(std::make_unique<Subfield::StringSubscript>("34"));
  elements.push_back(std::make_unique<Subfield::StringSubscript>("b \"test\""));
  elements.push_back(std::make_unique<Subfield::StringSubscript>("\"abc"));
  elements.push_back(std::make_unique<Subfield::StringSubscript>("abc\""));
  elements.push_back(std::make_unique<Subfield::StringSubscript>("ab\"cde"));
  return elements;
}

void testRoundTrip(const Subfield& path) {
  auto actual = Subfield(tokenize(path.toString()));
  ASSERT_TRUE(actual.valid());
  EXPECT_EQ(actual, path) << "at " << path.toString() << ", "
                          << actual.toString();
}

TEST(SubfieldTest, basic) {
  auto elements = createElements();
  for (auto& element : elements) {
    std::vector<std::unique_ptr<Subfield::PathElement>> newElements;
    newElements.push_back(std::make_unique<Subfield::NestedField>("a"));
    newElements.push_back(element->clone());
    testRoundTrip(Subfield(std::move(newElements)));
  }

  for (auto& element : elements) {
    for (auto& secondElement : elements) {
      std::vector<std::unique_ptr<Subfield::PathElement>> newElements;
      newElements.push_back(std::make_unique<Subfield::NestedField>("a"));
      newElements.push_back(element->clone());
      newElements.push_back(secondElement->clone());
      testRoundTrip(Subfield(std::move(newElements)));
    }
  }

  for (auto& element : elements) {
    for (auto& secondElement : elements) {
      for (auto& thirdElement : elements) {
        std::vector<std::unique_ptr<Subfield::PathElement>> newElements;
        newElements.push_back(std::make_unique<Subfield::NestedField>("a"));
        newElements.push_back(element->clone());
        newElements.push_back(secondElement->clone());
        newElements.push_back(thirdElement->clone());
        testRoundTrip(Subfield(std::move(newElements)));
      }
    }
  }

  ASSERT_FALSE(Subfield().valid());
  ASSERT_EQ(Subfield().toString(), "");
}

TEST(SubfieldTest, prefix) {
  EXPECT_FALSE(Subfield("a").isPrefix(Subfield("a")));
  EXPECT_TRUE(Subfield("a.b").isPrefix(Subfield("a.b.c")));
  EXPECT_TRUE(Subfield("a.b").isPrefix(Subfield("a.b[1]")));
  EXPECT_TRUE(Subfield("a.b").isPrefix(Subfield("a.b[\"d\"]")));
  EXPECT_FALSE(Subfield("a.c").isPrefix(Subfield("a.b.c")));
  EXPECT_FALSE(Subfield("a.b.c").isPrefix(Subfield("a.b")));
}

TEST(SubfieldTest, hash) {
  std::unordered_set<Subfield> subfields;
  subfields.emplace("a.b");
  subfields.emplace("a[\"b\"]");
  subfields.emplace("a.b.c");
  EXPECT_EQ(subfields.size(), 3);
  EXPECT_TRUE(subfields.find(Subfield("a.b")) != subfields.end());
  subfields.emplace("a.b.c");
  subfields.emplace("a[\"b\"]");
  EXPECT_EQ(subfields.size(), 3);
}

TEST(SubfieldTest, longSubscript) {
  Subfield subfield("a[3309189884973035076]");
  ASSERT_EQ(subfield.path().size(), 2);
  auto* longSubscript =
      dynamic_cast<const Subfield::LongSubscript*>(subfield.path()[1].get());
  ASSERT_TRUE(longSubscript);
  ASSERT_EQ(longSubscript->index(), 3309189884973035076);
}
