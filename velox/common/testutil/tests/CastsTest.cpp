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
#include <memory>

#include "velox/common/Casts.h"
#include "velox/common/base/tests/GTestUtils.h"

namespace facebook::velox {

namespace {

// Test classes for inheritance hierarchy
class BaseClass {
 public:
  virtual ~BaseClass() = default;
  virtual int getValue() const {
    return 42;
  }
};

class DerivedClass : public BaseClass {
 public:
  int getValue() const override {
    return 100;
  }

  int getDerivedValue() const {
    return 200;
  }
};

class AnotherDerivedClass : public BaseClass {
 public:
  int getValue() const override {
    return 300;
  }
};

class UnrelatedClass {
 public:
  virtual ~UnrelatedClass() = default;
  int getValue() const {
    return 999;
  }
};

} // namespace

class CastsTest : public ::testing::Test {
 protected:
  void SetUp() override {
    basePtr_ = std::make_shared<BaseClass>();
    derivedPtr_ = std::make_shared<DerivedClass>();
    anotherDerivedPtr_ = std::make_shared<AnotherDerivedClass>();
    unrelatedPtr_ = std::make_shared<UnrelatedClass>();

    baseUniquePtr_ = std::make_unique<BaseClass>();
    derivedUniquePtr_ = std::make_unique<DerivedClass>();

    baseRawPtr_ = new BaseClass();
    derivedRawPtr_ = new DerivedClass();
  }

  void TearDown() override {
    delete baseRawPtr_;
    delete derivedRawPtr_;
  }

  std::shared_ptr<BaseClass> basePtr_;
  std::shared_ptr<DerivedClass> derivedPtr_;
  std::shared_ptr<AnotherDerivedClass> anotherDerivedPtr_;
  std::shared_ptr<UnrelatedClass> unrelatedPtr_;

  std::unique_ptr<BaseClass> baseUniquePtr_;
  std::unique_ptr<DerivedClass> derivedUniquePtr_;

  BaseClass* baseRawPtr_;
  DerivedClass* derivedRawPtr_;
};

// Tests for checked_pointer_cast with shared_ptr
TEST_F(CastsTest, checkedPointerCastSharedPtrSuccess) {
  // Cast derived to base (should always work)
  auto result = checked_pointer_cast<BaseClass>(derivedPtr_);
  EXPECT_NE(result, nullptr);
  EXPECT_EQ(result->getValue(), 100);

  // Cast base to derived when it actually is derived
  std::shared_ptr<BaseClass> basePtrToDerived = derivedPtr_;
  auto derivedResult = checked_pointer_cast<DerivedClass>(basePtrToDerived);
  EXPECT_NE(derivedResult, nullptr);
  EXPECT_EQ(derivedResult->getValue(), 100);
  EXPECT_EQ(derivedResult->getDerivedValue(), 200);
}

TEST_F(CastsTest, checkedPointerCastSharedPtrFailure) {
  // Try to cast base to derived when it's not actually derived
  VELOX_ASSERT_THROW(checked_pointer_cast<DerivedClass>(basePtr_), "");

  // Try to cast to unrelated class
  VELOX_ASSERT_THROW(checked_pointer_cast<UnrelatedClass>(derivedPtr_), "");
}

TEST_F(CastsTest, checkedPointerCastSharedPtrNullInput) {
  std::shared_ptr<BaseClass> nullPtr;
  VELOX_ASSERT_THROW(checked_pointer_cast<DerivedClass>(nullPtr), "");
}

// Tests for checked_pointer_cast with unique_ptr
TEST_F(CastsTest, checkedPointerCastUniquePtrSuccess) {
  // Cast derived to base
  auto derivedForCast = std::make_unique<DerivedClass>();
  auto result = checked_pointer_cast<BaseClass>(std::move(derivedForCast));
  EXPECT_NE(result, nullptr);
  EXPECT_EQ(result->getValue(), 100);

  // Cast base to derived when it actually is derived
  std::unique_ptr<BaseClass> basePtrToDerived =
      std::make_unique<DerivedClass>();
  auto derivedResult =
      checked_pointer_cast<DerivedClass>(std::move(basePtrToDerived));
  EXPECT_NE(derivedResult, nullptr);
  EXPECT_EQ(derivedResult->getValue(), 100);
  EXPECT_EQ(derivedResult->getDerivedValue(), 200);
}

TEST_F(CastsTest, checkedPointerCastUniquePtrFailure) {
  // Try to cast base to derived when it's not actually derived
  auto baseForCast = std::make_unique<BaseClass>();
  VELOX_ASSERT_THROW(
      checked_pointer_cast<DerivedClass>(std::move(baseForCast)), "");
}

TEST_F(CastsTest, checkedPointerCastUniquePtrNullInput) {
  std::unique_ptr<BaseClass> nullPtr;
  VELOX_ASSERT_THROW(
      checked_pointer_cast<DerivedClass>(std::move(nullPtr)), "");
}

// Tests for checked_pointer_cast with raw pointers
TEST_F(CastsTest, checkedPointerCastRawPtrSuccess) {
  // Cast derived to base
  auto result = checked_pointer_cast<BaseClass>(derivedRawPtr_);
  EXPECT_NE(result, nullptr);
  EXPECT_EQ(result->getValue(), 100);

  // Cast base to derived when it actually is derived
  BaseClass* basePtrToDerived = derivedRawPtr_;
  auto derivedResult = checked_pointer_cast<DerivedClass>(basePtrToDerived);
  EXPECT_NE(derivedResult, nullptr);
  EXPECT_EQ(derivedResult->getValue(), 100);
  EXPECT_EQ(derivedResult->getDerivedValue(), 200);
}

TEST_F(CastsTest, checkedPointerCastRawPtrFailure) {
  // Try to cast base to derived when it's not actually derived
  VELOX_ASSERT_THROW(checked_pointer_cast<DerivedClass>(baseRawPtr_), "");
}

TEST_F(CastsTest, checkedPointerCastRawPtrNullInput) {
  BaseClass* nullPtr = nullptr;
  VELOX_ASSERT_THROW(checked_pointer_cast<DerivedClass>(nullPtr), "");
}

// Tests for static_unique_pointer_cast
TEST_F(CastsTest, staticUniquePointerCastSuccess) {
  // Create a unique_ptr to derived and cast to base
  auto derivedForCast = std::make_unique<DerivedClass>();
  auto originalPtr = derivedForCast.get();
  auto result =
      static_unique_pointer_cast<BaseClass>(std::move(derivedForCast));

  EXPECT_NE(result, nullptr);
  EXPECT_EQ(result.get(), originalPtr); // Should be the same pointer
  EXPECT_EQ(result->getValue(), 100);
}

TEST_F(CastsTest, staticUniquePointerCastNullInput) {
  std::unique_ptr<DerivedClass> nullPtr;
  VELOX_ASSERT_THROW(
      static_unique_pointer_cast<BaseClass>(std::move(nullPtr)), "");
}

// Tests for is_instance_of with shared_ptr
TEST_F(CastsTest, isInstanceOfSharedPtr) {
  // Test positive cases
  EXPECT_TRUE(is_instance_of<BaseClass>(derivedPtr_));
  EXPECT_TRUE(is_instance_of<DerivedClass>(derivedPtr_));
  EXPECT_TRUE(is_instance_of<BaseClass>(anotherDerivedPtr_));
  EXPECT_TRUE(is_instance_of<AnotherDerivedClass>(anotherDerivedPtr_));

  // Test negative cases
  EXPECT_FALSE(is_instance_of<DerivedClass>(basePtr_));
  EXPECT_FALSE(is_instance_of<AnotherDerivedClass>(derivedPtr_));
  EXPECT_FALSE(is_instance_of<DerivedClass>(anotherDerivedPtr_));
  EXPECT_FALSE(is_instance_of<UnrelatedClass>(derivedPtr_));
}

TEST_F(CastsTest, isInstanceOfSharedPtrNullInput) {
  std::shared_ptr<BaseClass> nullPtr;
  VELOX_ASSERT_THROW(is_instance_of<DerivedClass>(nullPtr), "");
}

// Tests for is_instance_of with unique_ptr
TEST_F(CastsTest, isInstanceOfUniquePtr) {
  // Test positive cases
  EXPECT_TRUE(is_instance_of<BaseClass>(derivedUniquePtr_));
  EXPECT_TRUE(is_instance_of<DerivedClass>(derivedUniquePtr_));

  // Test negative cases
  EXPECT_FALSE(is_instance_of<DerivedClass>(baseUniquePtr_));
  EXPECT_FALSE(is_instance_of<AnotherDerivedClass>(derivedUniquePtr_));
}

TEST_F(CastsTest, isInstanceOfUniquePtrNullInput) {
  std::unique_ptr<BaseClass> nullPtr;
  VELOX_ASSERT_THROW(is_instance_of<DerivedClass>(nullPtr), "");
}

// Tests for is_instance_of with raw pointers
TEST_F(CastsTest, isInstanceOfRawPtr) {
  // Test positive cases
  EXPECT_TRUE(is_instance_of<BaseClass>(derivedRawPtr_));
  EXPECT_TRUE(is_instance_of<DerivedClass>(derivedRawPtr_));

  // Test negative cases
  EXPECT_FALSE(is_instance_of<DerivedClass>(baseRawPtr_));
  EXPECT_FALSE(is_instance_of<AnotherDerivedClass>(derivedRawPtr_));
}

TEST_F(CastsTest, isInstanceOfRawPtrNullInput) {
  BaseClass* nullPtr = nullptr;
  VELOX_ASSERT_THROW(is_instance_of<DerivedClass>(nullPtr), "");
}

// Test error messages contain useful information
TEST_F(CastsTest, errorMessageContent) {
  try {
    checked_pointer_cast<DerivedClass>(basePtr_);
    FAIL() << "Expected VeloxException to be thrown";
  } catch (const VeloxException& e) {
    const std::string& message = e.message();
    // Check that the error message contains type information
    EXPECT_TRUE(message.find("Failed to cast") != std::string::npos);
    EXPECT_TRUE(message.find("BaseClass") != std::string::npos);
    EXPECT_TRUE(message.find("DerivedClass") != std::string::npos);
  }
}

// Test that successful casts preserve object identity
TEST_F(CastsTest, objectIdentityPreserved) {
  // For shared_ptr
  std::shared_ptr<BaseClass> basePtrToDerived = derivedPtr_;
  auto castedShared = checked_pointer_cast<DerivedClass>(basePtrToDerived);
  EXPECT_EQ(castedShared.get(), derivedPtr_.get());

  // For raw ptr
  BaseClass* basePtrToDerivedRaw = derivedRawPtr_;
  auto castedRaw = checked_pointer_cast<DerivedClass>(basePtrToDerivedRaw);
  EXPECT_EQ(castedRaw, derivedRawPtr_);
}

// Test exception safety for unique_ptr casting
TEST_F(CastsTest, uniquePtrExceptionSafety) {
  auto baseForCast = std::make_unique<BaseClass>();

  try {
    checked_pointer_cast<DerivedClass>(std::move(baseForCast));
    FAIL() << "Expected VeloxException to be thrown";
  } catch (const VeloxException&) {
    // The unique_ptr should have been restored and the object should still
    // exist We can't directly test this without access to the internal
    // implementation, but the test framework will detect memory leaks if the
    // object was lost
  }
}

} // namespace facebook::velox
