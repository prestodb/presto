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

#include <folly/base64.h>
#include <random>
#include "velox/common/base/tests/GTestUtils.h"
#include "velox/functions/lib/QuantileDigest.h"
#include "velox/functions/prestosql/tests/utils/FunctionBaseTest.h"
#include "velox/functions/prestosql/types/QDigestRegistration.h"
#include "velox/functions/prestosql/types/QDigestType.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::functions::test;
using namespace facebook::velox::functions::qdigest;

class QDigestFunctionsTest : public FunctionBaseTest {
 protected:
  void SetUp() override {
    FunctionBaseTest::SetUp();
    registerQDigestType();
  }

  template <typename T>
  std::string createQDigest(
      const std::vector<T>& values,
      double maxError = kMaxError) {
    std::allocator<T> allocator;
    QuantileDigest<T, std::allocator<T>> digest(allocator, maxError);

    for (const auto& value : values) {
      digest.add(value, 1.0);
    }

    const auto serializedSize = digest.serializedByteSize();
    std::vector<char> serializedData(serializedSize);
    digest.serialize(serializedData.data());
    return std::string(serializedData.begin(), serializedData.end());
  }

  // Type definitions
  const TypePtr QDIGEST_DOUBLE = QDIGEST(DOUBLE());
  const TypePtr QDIGEST_BIGINT = QDIGEST(BIGINT());
  const TypePtr QDIGEST_REAL = QDIGEST(REAL());
  const TypePtr ARRAY_QDIGEST_DOUBLE = ARRAY(QDIGEST(DOUBLE()));
  static constexpr double kMaxError = 0.01;
};

TEST_F(QDigestFunctionsTest, valueAtQuantileDouble) {
  const auto valueAtQuantile = [&](const std::optional<std::string>& input,
                                   const std::optional<double>& quantile) {
    return evaluateOnce<double>(
        "value_at_quantile(c0, c1)", QDIGEST_DOUBLE, input, quantile);
  };

  std::vector<double> values;
  for (int i = 1; i <= 1000; i++) {
    values.push_back(static_cast<double>(i + 0.1 * i));
  }
  std::string digest = createQDigest(values);

  ASSERT_NEAR(valueAtQuantile(digest, 0.0).value(), 1.1, kMaxError);
  ASSERT_NEAR(valueAtQuantile(digest, 0.002).value(), 3.3, kMaxError);
  ASSERT_NEAR(valueAtQuantile(digest, 0.08).value(), 89.1, kMaxError);
  ASSERT_NEAR(valueAtQuantile(digest, 0.109).value(), 121.0, kMaxError);
  ASSERT_NEAR(valueAtQuantile(digest, 0.25).value(), 276.1, kMaxError);
  ASSERT_NEAR(valueAtQuantile(digest, 0.311).value(), 343.2, kMaxError);
  ASSERT_NEAR(valueAtQuantile(digest, 0.5).value(), 551.1, kMaxError);
  ASSERT_NEAR(valueAtQuantile(digest, 0.75).value(), 826.1, kMaxError);
  ASSERT_NEAR(valueAtQuantile(digest, 0.811).value(), 893.2, kMaxError);
  ASSERT_NEAR(valueAtQuantile(digest, 0.991).value(), 1091.2, kMaxError);
  ASSERT_NEAR(valueAtQuantile(digest, 0.9999).value(), 1100.0, kMaxError);
  ASSERT_NEAR(valueAtQuantile(digest, 1.0).value(), 1100.0, kMaxError);
}

TEST_F(QDigestFunctionsTest, valueAtQuantileBigint) {
  const auto valueAtQuantile = [&](const std::optional<std::string>& input,
                                   const std::optional<double>& quantile) {
    return evaluateOnce<int64_t>(
        "value_at_quantile(c0, c1)", QDIGEST_BIGINT, input, quantile);
  };

  std::vector<int64_t> values;
  for (int64_t i = 1; i <= 1000; i++) {
    values.push_back(i);
  }
  std::string digest = createQDigest(values);
  constexpr double kMaxError = 0.01;

  ASSERT_NEAR(valueAtQuantile(digest, 0.0).value(), 1, kMaxError);
  ASSERT_NEAR(valueAtQuantile(digest, 0.002).value(), 3, kMaxError);
  ASSERT_NEAR(valueAtQuantile(digest, 0.08).value(), 81, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.109).value(), 110, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.25).value(), 251, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.311).value(), 312, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.5).value(), 501, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.75).value(), 751, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.811).value(), 812, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.991).value(), 992, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.9999).value(), 1000, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 1.0).value(), 1000, 0.1);
}

TEST_F(QDigestFunctionsTest, valueAtQuantileReal) {
  const auto valueAtQuantile = [&](const std::optional<std::string>& input,
                                   const std::optional<double>& quantile) {
    return evaluateOnce<float>(
        "value_at_quantile(c0, c1)", QDIGEST_REAL, input, quantile);
  };

  std::vector<float> values;
  for (int i = 1; i <= 1000; i++) {
    values.push_back(static_cast<float>(i + 0.1 * i));
  }
  std::string digest = createQDigest(values);

  ASSERT_NEAR(valueAtQuantile(digest, 0.0).value(), 1.1f, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.002).value(), 3.3f, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.08).value(), 89.1f, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.109).value(), 121.0f, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.25).value(), 276.1f, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.311).value(), 343.2f, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.5).value(), 551.1f, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.75).value(), 826.1f, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.811).value(), 893.2f, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.991).value(), 1091.2f, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 0.9999).value(), 1100.0f, 0.1);
  ASSERT_NEAR(valueAtQuantile(digest, 1.0).value(), 1100.0f, 0.1);
}

TEST_F(QDigestFunctionsTest, valueAtQuantileOutOfRange) {
  const auto valueAtQuantile = [&](const std::optional<std::string>& input,
                                   const std::optional<double>& quantile) {
    return evaluateOnce<double>(
        "value_at_quantile(c0, c1)", QDIGEST_DOUBLE, input, quantile);
  };

  std::vector<double> values = {1.0, 2.0, 3.0};
  std::string digest = createQDigest(values);

  EXPECT_THROW(valueAtQuantile(digest, -0.1), VeloxUserError);
  EXPECT_THROW(valueAtQuantile(digest, 1.1), VeloxUserError);
}

TEST_F(QDigestFunctionsTest, valueAtQuantileNullInput) {
  const auto valueAtQuantile = [&](const std::optional<std::string>& input,
                                   const std::optional<double>& quantile) {
    return evaluateOnce<double>(
        "value_at_quantile(c0, c1)", QDIGEST_DOUBLE, input, quantile);
  };

  ASSERT_EQ(std::nullopt, valueAtQuantile(std::nullopt, 0.5));
}

TEST_F(QDigestFunctionsTest, valuesAtQuantilesDouble) {
  std::vector<double> values;
  for (int i = 1; i <= 1000; i++) {
    values.push_back(static_cast<double>(i + 0.1 * i));
  }
  std::string digest = createQDigest(values);

  auto c0 = makeFlatVector<std::string>({digest}, QDIGEST_DOUBLE);
  auto c1 = makeNullableArrayVector<double>(
      {{0,
        0.002,
        0.08,
        0.109,
        0.25,
        0.311,
        0.5,
        0.75,
        0.811,
        0.991,
        0.9999,
        1.0}});

  auto result =
      evaluate("values_at_quantiles(c0, c1)", makeRowVector({c0, c1}));
  auto arrayResult = result->as<ArrayVector>();

  auto elements = arrayResult->elements()->as<FlatVector<double>>();
  ASSERT_EQ(elements->size(), 12);
  std::array<double, 12> rst{
      1.1,
      3.3,
      89.1,
      121,
      276.1,
      343.2,
      551.1,
      826.1,
      893.2,
      1091.2,
      1100,
      1100};
  for (auto i = 0; i < 12; i++) {
    ASSERT_NEAR(elements->valueAt(i), rst[i], 1.0);
  }
}

TEST_F(QDigestFunctionsTest, valuesAtQuantilesBigint) {
  std::vector<int64_t> values;
  for (int64_t i = 1; i <= 1000; i++) {
    values.push_back(i);
  }
  std::string digest = createQDigest(values);

  auto c0 = makeFlatVector<std::string>({digest}, QDIGEST_BIGINT);
  auto c1 = makeNullableArrayVector<double>(
      {{0,
        0.002,
        0.08,
        0.109,
        0.25,
        0.311,
        0.5,
        0.75,
        0.811,
        0.991,
        0.9999,
        1.0}});

  auto result =
      evaluate("values_at_quantiles(c0, c1)", makeRowVector({c0, c1}));
  auto arrayResult = result->as<ArrayVector>();

  auto elements = arrayResult->elements()->as<FlatVector<int64_t>>();
  ASSERT_EQ(elements->size(), 12);
  std::array<int64_t, 12> rst{
      1, 3, 81, 110, 251, 312, 501, 751, 812, 992, 1000, 1000};
  for (auto i = 0; i < 12; i++) {
    ASSERT_NEAR(elements->valueAt(i), rst[i], 1.0);
  }
}

TEST_F(QDigestFunctionsTest, valuesAtQuantilesReal) {
  std::vector<float> values;
  for (int i = 1; i <= 1000; i++) {
    values.push_back(static_cast<float>(i) + i * 0.1f);
  }
  std::string digest = createQDigest(values);

  auto c0 = makeFlatVector<std::string>({digest}, QDIGEST_REAL);
  auto c1 = makeNullableArrayVector<double>(
      {{0,
        0.002,
        0.08,
        0.109,
        0.25,
        0.311,
        0.5,
        0.75,
        0.811,
        0.991,
        0.9999,
        1.0}});

  auto result =
      evaluate("values_at_quantiles(c0, c1)", makeRowVector({c0, c1}));
  auto arrayResult = result->as<ArrayVector>();
  auto elements = arrayResult->elements()->as<FlatVector<float>>();
  ASSERT_EQ(elements->size(), 12);
  std::array<float, 12> rst{
      1.1,
      3.3,
      89.1,
      121,
      276.1,
      343.2,
      551.1,
      826.1,
      893.2,
      1091.2,
      1100,
      1100};
  for (auto i = 0; i < 12; i++) {
    ASSERT_NEAR(elements->valueAt(i), rst[i], 0.01);
  }
}

TEST_F(QDigestFunctionsTest, valuesAtQuantilesWithNulls) {
  std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0};
  std::string digest = createQDigest(values);

  auto c0 = makeFlatVector<std::string>({digest}, QDIGEST_DOUBLE);
  std::vector<std::vector<std::optional<double>>> data = {
      {0, std::nullopt, std::nullopt, 1}};
  auto c1 = makeNullableArrayVector<double>(data);

  ASSERT_THROW(
      evaluate("values_at_quantiles(c0, c1)", makeRowVector({c0, c1})),
      VeloxUserError);
}

TEST_F(QDigestFunctionsTest, valuesAtQuantilesEmptyArray) {
  std::vector<double> values = {1.0, 2.0, 3.0};
  std::string digest = createQDigest(values);

  auto arg0 = makeFlatVector<std::string>({digest}, QDIGEST_DOUBLE);
  std::vector<std::vector<std::optional<double>>> emptyData = {{}};
  auto arg1 = makeNullableArrayVector<double>(emptyData);

  auto result =
      evaluate("values_at_quantiles(c0, c1)", makeRowVector({arg0, arg1}));
  auto arrayResult = result->as<ArrayVector>();

  ASSERT_EQ(arrayResult->sizeAt(0), 0);
}

TEST_F(QDigestFunctionsTest, quantileAtValueBigint) {
  const auto quantileAtValue = [&](const std::optional<std::string>& input,
                                   const std::optional<int64_t>& value) {
    return evaluateOnce<double>(
        "quantile_at_value(c0, c1)", QDIGEST_BIGINT, input, value);
  };

  std::vector<int64_t> values;
  for (int64_t i = 1; i <= 1000; i++) {
    values.push_back(i);
  }
  std::string digest = createQDigest(values, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 1).value(), 0.0, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 2).value(), 0.001, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 3).value(), 0.002, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 333).value(), 0.332, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 500).value(), 0.499, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 666).value(), 0.665, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 998).value(), 0.997, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 999).value(), 0.998, kMaxError);
  ASSERT_EQ(quantileAtValue(digest, 0), std::nullopt);
  ASSERT_EQ(quantileAtValue(digest, 1001), std::nullopt);
}

TEST_F(QDigestFunctionsTest, quantileAtValueDouble) {
  const auto quantileAtValue = [&](const std::optional<std::string>& input,
                                   const std::optional<double>& value) {
    return evaluateOnce<double>(
        "quantile_at_value(c0, c1)", QDIGEST_DOUBLE, input, value);
  };

  std::vector<double> values;
  for (int i = 1; i <= 1000; i++) {
    values.push_back(static_cast<double>(i + 0.1 * i));
  }

  std::string digest = createQDigest(values, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 1.1).value(), 0.0, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 1.111).value(), 0.001, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 2.001).value(), 0.001, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 333.33).value(), 0.303, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 500.001).value(), 0.454, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 666.666).value(), 0.606, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 998.999).value(), 0.908, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 1099.999).value(), 0.999, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 1100).value(), 0.999, kMaxError);
  ASSERT_EQ(quantileAtValue(digest, -1.1), std::nullopt);
  ASSERT_EQ(quantileAtValue(digest, 1100.001), std::nullopt);
}

TEST_F(QDigestFunctionsTest, quantileAtValueReal) {
  const auto quantileAtValue = [&](const std::optional<std::string>& input,
                                   const std::optional<float>& value) {
    return evaluateOnce<double>(
        "quantile_at_value(c0, c1)", QDIGEST_REAL, input, value);
  };

  std::vector<float> values;
  for (int i = 1; i <= 1000; i++) {
    values.push_back(static_cast<float>(i + 0.1 * i));
  }

  std::string digest = createQDigest(values, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 1.1).value(), 0.0, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 1.111).value(), 0.001, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 2.001).value(), 0.001, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 333.33).value(), 0.303, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 500.001).value(), 0.454, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 666.666).value(), 0.606, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 998.999).value(), 0.908, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 1099.999).value(), 0.999, kMaxError);
  ASSERT_NEAR(quantileAtValue(digest, 1100).value(), 0.999, kMaxError);
  ASSERT_EQ(quantileAtValue(digest, -1.1), std::nullopt);
  ASSERT_EQ(quantileAtValue(digest, 1100.001), std::nullopt);
}

TEST_F(QDigestFunctionsTest, quantileAtValueEmptyDigest) {
  const auto quantileAtValueBigint =
      [&](const std::optional<std::string>& input,
          const std::optional<int64_t>& value) {
        return evaluateOnce<double>(
            "quantile_at_value(c0, c1)", QDIGEST_BIGINT, input, value);
      };

  const auto quantileAtValueDouble =
      [&](const std::optional<std::string>& input,
          const std::optional<double>& value) {
        return evaluateOnce<double>(
            "quantile_at_value(c0, c1)", QDIGEST_DOUBLE, input, value);
      };

  std::string emptyDigestBigint = createQDigest(std::vector<int64_t>{});
  std::string emptyDigestDouble = createQDigest(std::vector<double>{});
  std::string emptyDigestReal = createQDigest(std::vector<float>{});

  ASSERT_EQ(quantileAtValueBigint(emptyDigestBigint, 1), std::nullopt);
  ASSERT_EQ(quantileAtValueDouble(emptyDigestDouble, 1), std::nullopt);
  ASSERT_EQ(quantileAtValueDouble(emptyDigestReal, 51), std::nullopt);
}

TEST_F(QDigestFunctionsTest, quantileAtValueNullInputs) {
  const auto quantileAtValueBigint =
      [&](const std::optional<std::string>& input,
          const std::optional<int64_t>& value) {
        return evaluateOnce<double>(
            "quantile_at_value(c0, c1)", QDIGEST_BIGINT, input, value);
      };

  const auto quantileAtValueDouble =
      [&](const std::optional<std::string>& input,
          const std::optional<double>& value) {
        return evaluateOnce<double>(
            "quantile_at_value(c0, c1)", QDIGEST_DOUBLE, input, value);
      };

  const auto quantileAtValueReal = [&](const std::optional<std::string>& input,
                                       const std::optional<float>& value) {
    return evaluateOnce<double>(
        "quantile_at_value(c0, c1)", QDIGEST_REAL, input, value);
  };

  // Create valid digests for testing
  std::vector<int64_t> bigintValues = {1, 2, 3, 4, 5};
  std::vector<double> doubleValues = {1.1, 2.2, 3.3, 4.4, 5.5};
  std::vector<float> realValues = {1.1f, 2.2f, 3.3f, 4.4f, 5.5f};

  std::string digestBigint = createQDigest(bigintValues);
  std::string digestDouble = createQDigest(doubleValues);
  std::string digestReal = createQDigest(realValues);

  ASSERT_EQ(quantileAtValueBigint(std::nullopt, 3), std::nullopt);
  ASSERT_EQ(quantileAtValueDouble(std::nullopt, 3.3), std::nullopt);
  ASSERT_EQ(quantileAtValueReal(std::nullopt, 3.3f), std::nullopt);
  ASSERT_EQ(quantileAtValueBigint(digestBigint, std::nullopt), std::nullopt);
  ASSERT_EQ(quantileAtValueDouble(digestDouble, std::nullopt), std::nullopt);
  ASSERT_EQ(quantileAtValueReal(digestReal, std::nullopt), std::nullopt);
  ASSERT_EQ(quantileAtValueBigint(std::nullopt, std::nullopt), std::nullopt);
  ASSERT_EQ(quantileAtValueDouble(std::nullopt, std::nullopt), std::nullopt);
  ASSERT_EQ(quantileAtValueReal(std::nullopt, std::nullopt), std::nullopt);
}

TEST_F(QDigestFunctionsTest, scaleQDigestDouble) {
  std::vector<double> values = {1.0, 2.0, 3.0, 4.0, 5.0};
  std::string digest = createQDigest(values);

  auto arg0 = makeNullableFlatVector<std::string>({digest}, QDIGEST_DOUBLE);

  auto scaleUpResult =
      evaluate("scale_qdigest(c0, 2.0)", makeRowVector({arg0}));

  auto scaleDownResult =
      evaluate("scale_qdigest(c0, 0.5)", makeRowVector({scaleUpResult}));

  test::assertEqualVectors(arg0, scaleDownResult);
}

TEST_F(QDigestFunctionsTest, scaleQDigestBigint) {
  std::vector<int64_t> values = {1, 2, 3, 4, 5};
  std::string digest = createQDigest(values);

  auto arg0 = makeNullableFlatVector<std::string>({digest}, QDIGEST_BIGINT);

  // Scale up by 2
  auto scaleUpResult =
      evaluate("scale_qdigest(c0, 2.0)", makeRowVector({arg0}));

  // Scale down by 0.5
  auto scaleDownResult =
      evaluate("scale_qdigest(c0, 0.5)", makeRowVector({scaleUpResult}));

  // Should be approximately equal to original
  test::assertEqualVectors(arg0, scaleDownResult);
}

TEST_F(QDigestFunctionsTest, scaleQDigestReal) {
  std::vector<float> values = {1.0f, 2.0f, 3.0f, 4.0f, 5.0f};
  std::string digest = createQDigest(values);

  auto arg0 = makeNullableFlatVector<std::string>({digest}, QDIGEST_REAL);

  auto scaleUpResult =
      evaluate("scale_qdigest(c0, 2.5)", makeRowVector({arg0}));

  auto scaleDownResult =
      evaluate("scale_qdigest(c0, 0.4)", makeRowVector({scaleUpResult}));

  test::assertEqualVectors(arg0, scaleDownResult);
}

TEST_F(QDigestFunctionsTest, scaleQDigestNegative) {
  std::vector<double> values = {1.0, 2.0, 3.0};
  std::string digest = createQDigest(values);

  const auto scaleQDigest = [&](const std::optional<std::string>& input,
                                const std::optional<double>& scale) {
    return evaluateOnce<std::string>(
        "scale_qdigest(c0, c1)", QDIGEST_DOUBLE, input, scale);
  };

  VELOX_ASSERT_THROW(
      scaleQDigest(digest, -1.0), "Scale factor should be positive.");
  VELOX_ASSERT_THROW(
      scaleQDigest(digest, 0.0), "Scale factor should be positive.");
}

TEST_F(QDigestFunctionsTest, scaleQDigestNull) {
  const auto scaleQDigest = [&](const std::optional<std::string>& input,
                                const std::optional<double>& scale) {
    return evaluateOnce<std::string>(
        "scale_qdigest(c0, c1)", QDIGEST_DOUBLE, input, scale);
  };

  ASSERT_EQ(std::nullopt, scaleQDigest(std::nullopt, 2.0));
  ASSERT_EQ(std::nullopt, scaleQDigest("digest", std::nullopt));
}
