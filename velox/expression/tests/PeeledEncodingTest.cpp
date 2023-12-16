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

#include "gtest/gtest.h"

#include "velox/expression/EvalCtx.h"
#include "velox/expression/PeeledEncoding.h"
#include "velox/vector/fuzzer/VectorFuzzer.h"
#include "velox/vector/tests/utils/VectorTestBase.h"

using namespace facebook::velox;
using namespace facebook::velox::exec;
using namespace facebook::velox::test;

class PeeledEncodingTest : public testing::Test, public VectorTestBase {
 public:
  struct DictionaryWrap {
    BufferPtr indices;
    BufferPtr nulls;
    int size;
  };

 protected:
  static void SetUpTestCase() {
    memory::MemoryManager::testingSetInstance({});
  }

  static DictionaryWrap generateDictionaryWrap(
      VectorFuzzer& fuzzer,
      vector_size_t size) {
    return {fuzzer.fuzzIndices(size, size), fuzzer.fuzzNulls(size), size};
  }

  // Utility function to wrap an arbitrary number of dictionary wrappings. Takes
  // the 'base' vector to wrap and a list of references to DictionaryWrap
  // objects. The base is then wrapped with those layers in the same order as
  // encountered in the list.
  VectorPtr wrapInDictionaryLayers(
      VectorPtr base,
      std::vector<DictionaryWrap*> layers) {
    for (DictionaryWrap* wrap : layers) {
      VELOX_CHECK_NOT_NULL(wrap);
      base = BaseVector::wrapInDictionary(
          wrap->nulls, wrap->indices, wrap->size, base);
    }
    return base;
  }

  // Utility function that peels a given 'wrappedVector'. Takes the number of
  // wraps to peel and a vector which is expected to have that many number of
  // wraps.
  VectorPtr peelWrappings(int numWrappingsToPeel, VectorPtr wrappedVector) {
    if (numWrappingsToPeel == 0 ||
        !PeeledEncoding::isPeelable(wrappedVector->encoding())) {
      return wrappedVector;
    } else {
      return peelWrappings(
          numWrappingsToPeel - 1, wrappedVector->valueVector());
    }
  }

  void SetUp() override {
    VectorFuzzer::Options options;
    options.nullRatio = 0.3;
    fuzzer = std::make_shared<VectorFuzzer>(options, pool());

    dictWrap1 = generateDictionaryWrap(*fuzzer, vectorSize_);
    dictWrap2 = generateDictionaryWrap(*fuzzer, vectorSize_);
    dictWrap3 = generateDictionaryWrap(*fuzzer, vectorSize_);

    flat1 = fuzzer->fuzzFlat(INTEGER());
    flat2 = fuzzer->fuzzFlat(INTEGER());
    const1 = fuzzer->fuzzConstant(INTEGER());
    const2 = fuzzer->fuzzConstant(INTEGER());
    complexConst = fuzzer->fuzzConstant(ARRAY(INTEGER()));
  }

  static const vector_size_t vectorSize_ = 100;
  core::ExecCtx execCtx_{pool_.get(), nullptr};
  std::shared_ptr<VectorFuzzer> fuzzer;
  // Common test data used by tests.
  DictionaryWrap dictWrap1;
  DictionaryWrap dictWrap2;
  DictionaryWrap dictWrap3;
  VectorPtr flat1;
  VectorPtr flat2;
  VectorPtr const1;
  VectorPtr const2;
  VectorPtr complexConst;
};

struct ParamType {
  std::string testName;
  SelectivityVector rows;
};

class PeeledEncodingBasicTests : public PeeledEncodingTest,
                                 public testing::WithParamInterface<ParamType> {
 public:
  static std::vector<ParamType> generateTestParams() {
    std::vector<ParamType> params;
    // One selected.
    SelectivityVector rows(vectorSize_, false);
    rows.setValid(1, true);
    rows.updateBounds();
    params.push_back({"one_row_selected", rows});

    // Trailing rows selected.
    rows.clearAll();
    rows.setValidRange(vectorSize_ / 3, vectorSize_, true);
    rows.updateBounds();
    params.push_back({"trailing_rows_selected", rows});

    // Trailing rows not selected.
    rows.clearAll();
    rows.setValidRange(0, (2 * vectorSize_) / 3, true);
    rows.updateBounds();
    params.push_back({"trailing_rows_not_selected", rows});

    // Rows in the middle selected.
    rows.clearAll();
    rows.setValidRange(vectorSize_ / 3, (2 * vectorSize_) / 3, true);
    rows.updateBounds();
    params.push_back({"rows_in_the_middle_selected", rows});

    // Rows in the middle not selected.
    rows.clearAll();
    rows.setValidRange(0, vectorSize_ / 3, true);
    rows.setValidRange((2 * vectorSize_) / 3, vectorSize_, true);
    rows.updateBounds();
    params.push_back({"rows_in_the_middle_not_selected", rows});

    // All selected.
    rows.setAll();
    params.push_back({"all_rows_selected", rows});

    return params;
  }
};

INSTANTIATE_TEST_SUITE_P(
    PeeledEncodingTests,
    PeeledEncodingBasicTests,
    testing::ValuesIn(PeeledEncodingBasicTests::generateTestParams()),
    [](const ::testing::TestParamInfo<PeeledEncodingBasicTests::ParamType>&
           info) { return info.param.testName; });

TEST_P(PeeledEncodingBasicTests, allCommonDictionaryLayers) {
  LocalDecodedVector localDecodedVector(execCtx_);
  const SelectivityVector& rows = GetParam().rows;
  // 1. Common dictionary layers are peeled
  //    Input Vectors: Dict1(Dict2(Flat1)), Dict1(Dict2(Const1)),
  //                   Dict1(Dict2(Dict3(Flat2)))
  //    Peeled Vectors: Flat, Const1, Dict3(Flat2)
  //    Peel: Dict1(Dict2) => collapsed into one dictionary

  auto input1 = wrapInDictionaryLayers(flat1, {&dictWrap2, &dictWrap1});
  auto input2 = wrapInDictionaryLayers(const1, {&dictWrap2, &dictWrap1});
  auto input3 =
      wrapInDictionaryLayers(flat2, {&dictWrap3, &dictWrap2, &dictWrap1});
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding = PeeledEncoding::peel(
      {input1, input2, input3}, rows, localDecodedVector, true, peeledVectors);
  ASSERT_EQ(peeledVectors.size(), 3);
  ASSERT_EQ(peeledEncoding->wrapEncoding(), VectorEncoding::Simple::DICTIONARY);
  ASSERT_EQ(peeledVectors[0].get(), flat1.get());
  ASSERT_EQ(peeledVectors[1].get(), const1.get());
  ASSERT_EQ(peeledVectors[2].get(), peelWrappings(2, input3).get());
  assertEqualVectors(
      input1, peeledEncoding->wrap(flat1->type(), pool(), flat1, rows), rows);
}

TEST_P(PeeledEncodingBasicTests, someCommonDictionaryLayers) {
  LocalDecodedVector localDecodedVector(execCtx_);
  const SelectivityVector& rows = GetParam().rows;
  // 2. Common dictionary layers are peeled
  //    Input Vectors: Dict1(Dict2(Flat1)), Dict1(Const1)),
  //                   Dict1(Dict2(Dict3(Flat2)))
  //    Peeled Vectors: Dict2(Flat), Const1, Dict2(Dict3(Flat2))
  //    Peel: Dict1

  auto input1 = wrapInDictionaryLayers(flat1, {&dictWrap2, &dictWrap1});
  auto input2 = wrapInDictionaryLayers(const1, {&dictWrap1});
  auto input3 =
      wrapInDictionaryLayers(flat2, {&dictWrap3, &dictWrap2, &dictWrap1});
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding = PeeledEncoding::peel(
      {input1, input2, input3}, rows, localDecodedVector, true, peeledVectors);
  ASSERT_EQ(peeledVectors.size(), 3);
  ASSERT_EQ(peeledEncoding->wrapEncoding(), VectorEncoding::Simple::DICTIONARY);
  ASSERT_EQ(peeledVectors[0].get(), peelWrappings(1, input1).get());
  ASSERT_EQ(peeledVectors[1].get(), peelWrappings(1, input2).get());
  ASSERT_EQ(peeledVectors[2].get(), peelWrappings(1, input3).get());
  assertEqualVectors(
      wrapInDictionaryLayers(flat1, {&dictWrap1}),
      peeledEncoding->wrap(flat1->type(), pool(), flat1, rows),
      rows);
}

TEST_P(PeeledEncodingBasicTests, commonDictionaryLayersAndAConstant) {
  LocalDecodedVector localDecodedVector(execCtx_);
  const SelectivityVector& rows = GetParam().rows;
  // 3. Common dictionary layers are peeled while constant is ignored
  //    (since all valid rows translated via the common dictionary layers would
  //    point to the same constant index)
  //    Input Vectors: Dict1(Dict2(Flat1)), Const1,
  //                   Dict1(Dict2(Dict3(Flat2)))
  //    Peeled Vectors: Flat, Const1, Dict3(Flat2)
  //    Peel: Dict1(Dict2) => collapsed into one dictionary
  auto input1 = wrapInDictionaryLayers(flat1, {&dictWrap2, &dictWrap1});
  auto input2 = const1;
  auto input3 =
      wrapInDictionaryLayers(flat2, {&dictWrap3, &dictWrap2, &dictWrap1});
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding = PeeledEncoding::peel(
      {input1, input2, input3}, rows, localDecodedVector, true, peeledVectors);
  ASSERT_EQ(peeledVectors.size(), 3);
  ASSERT_EQ(peeledEncoding->wrapEncoding(), VectorEncoding::Simple::DICTIONARY);
  ASSERT_EQ(peeledVectors[0].get(), flat1.get());
  if (peeledVectors[1].get() != const1.get()) {
    // In case the constant is resized to match other peeledVector's size.
    assertEqualVectors(peeledVectors[1], const1, rows);
  }
  ASSERT_EQ(peeledVectors[2].get(), peelWrappings(2, input3).get());
  assertEqualVectors(
      input1, peeledEncoding->wrap(flat1->type(), pool(), flat1, rows), rows);
}

TEST_P(PeeledEncodingBasicTests, singleConstantEncodedVector) {
  LocalDecodedVector localDecodedVector(execCtx_);
  const SelectivityVector& rows = GetParam().rows;
  /// 4. A single vector with constant encoding layer over a complex vector can
  ///    be peeled.
  ///    Input Vectors: Const1(Complex1)
  ///    Peeled Vectors: Complex
  ///    peel: Const1
  auto input1 = complexConst;
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding = PeeledEncoding::peel(
      {input1}, rows, localDecodedVector, true, peeledVectors);
  ASSERT_EQ(peeledVectors.size(), 1);
  ASSERT_EQ(peeledEncoding->wrapEncoding(), VectorEncoding::Simple::CONSTANT);
  ASSERT_EQ(peeledVectors[0].get(), peelWrappings(1, input1).get());
  ASSERT_EQ(peeledVectors[0]->encoding(), VectorEncoding::Simple::ARRAY);
  assertEqualVectors(
      input1,
      peeledEncoding->wrap(
          complexConst->type(), pool(), complexConst->valueVector(), rows),
      rows);
}

TEST_P(PeeledEncodingBasicTests, dictionaryOverConstantOverNullComplex) {
  LocalDecodedVector localDecodedVector(execCtx_);
  const SelectivityVector& rows = GetParam().rows;
  // 5. A single vector with arbitrary dictionary layers over a constant
  //    encoding layer over a NULL complex vector can be peeled and the peels
  //    can be merged into a single constant layer. Input Vectors:
  //    Dict1(Const1(NullComplex)) Peeled Vectors: Null Complex vector Peel:
  //    Const(generic constant index)
  auto nullComplexConst =
      BaseVector::createNullConstant(complexConst->type(), vectorSize_, pool());
  auto input1 = wrapInDictionaryLayers(nullComplexConst, {&dictWrap1});
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding = PeeledEncoding::peel(
      {input1}, rows, localDecodedVector, true, peeledVectors);
  ASSERT_EQ(peeledVectors.size(), 1);
  ASSERT_EQ(peeledEncoding->wrapEncoding(), VectorEncoding::Simple::CONSTANT);
  ASSERT_EQ(peeledVectors[0].get(), peelWrappings(2, input1).get());
  ASSERT_EQ(peeledVectors[0]->encoding(), VectorEncoding::Simple::ARRAY);
  assertEqualVectors(
      input1,
      peeledEncoding->wrap(
          complexConst->type(), pool(), nullComplexConst->valueVector(), rows),
      rows);
}

TEST_P(PeeledEncodingBasicTests, dictionaryOverConstantOverComplex) {
  LocalDecodedVector localDecodedVector(execCtx_);
  const SelectivityVector& rows = GetParam().rows;
  // 6. A single vector with arbitrary dictionary layers over a constant
  //    encoding layer over a Non-NULL complex vector.
  //    Input Vectors: Dict1(Const1(NonNullComplex))
  //    Peeled Vectors: Non-Null Complex vector
  //    Peel: Dict1(Const) => collapsed into one dictionary
  auto nonNullComplexConst = BaseVector::wrapInConstant(
      vectorSize_, 0, fuzzer->fuzzNotNull(ARRAY(INTEGER()), 1));
  auto input1 = wrapInDictionaryLayers(nonNullComplexConst, {&dictWrap1});
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding = PeeledEncoding::peel(
      {input1}, rows, localDecodedVector, true, peeledVectors);
  ASSERT_EQ(peeledVectors.size(), 1);
  ASSERT_EQ(peeledEncoding->wrapEncoding(), VectorEncoding::Simple::DICTIONARY);
  ASSERT_EQ(peeledVectors[0].get(), peelWrappings(2, input1).get());
  ASSERT_EQ(peeledVectors[0]->encoding(), VectorEncoding::Simple::ARRAY);
  assertEqualVectors(
      input1,
      peeledEncoding->wrap(
          nonNullComplexConst->type(),
          pool(),
          nonNullComplexConst->valueVector(),
          rows),
      rows);
}

TEST_P(PeeledEncodingBasicTests, allConstant) {
  LocalDecodedVector localDecodedVector(execCtx_);
  const SelectivityVector& rows = GetParam().rows;
  // 7. All constant inputs where the peel is just a constant pointing to the
  //    first valid row. The peel itself is irrelevant but allows us to
  //    translate input rows into a single row.
  //    Input Vectors: Const1(Complex1), Const2, Const3
  //    Peeled Vectors: Const1(Complex1), Const2, Const3
  //    Peel: Const(generic constant index)
  auto input1 = complexConst;
  auto input2 = const1;
  auto input3 = const2;
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding = PeeledEncoding::peel(
      {input1, input2, input3}, rows, localDecodedVector, true, peeledVectors);
  ASSERT_EQ(peeledVectors.size(), 3);
  ASSERT_EQ(peeledEncoding->wrapEncoding(), VectorEncoding::Simple::CONSTANT);
  ASSERT_EQ(peeledVectors[0].get(), complexConst.get());
  ASSERT_EQ(peeledVectors[1].get(), const1.get());
  ASSERT_EQ(peeledVectors[2].get(), const2.get());
  assertEqualVectors(
      input1,
      peeledEncoding->wrap(complexConst->type(), pool(), complexConst, rows),
      rows);
}

TEST_P(PeeledEncodingBasicTests, emptyInputVectors) {
  LocalDecodedVector localDecodedVector(execCtx_);
  const SelectivityVector& rows = GetParam().rows;
  // 8. No inputs. This is considered as constant encoding since its expected
  //    to produce the same result for all valid rows.
  //    Input Vectors: <empty>
  //    Peeled Vectors: <empty>
  //    Peel: Const(generic constant index)
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding =
      PeeledEncoding::peel({}, rows, localDecodedVector, true, peeledVectors);
  ASSERT_EQ(peeledVectors.size(), 0);
  ASSERT_EQ(peeledEncoding->wrapEncoding(), VectorEncoding::Simple::CONSTANT);
  assertEqualVectors(
      const1, peeledEncoding->wrap(const1->type(), pool(), const1, rows), rows);
}

TEST_P(PeeledEncodingBasicTests, dictionaryLayersHavingNulls) {
  LocalDecodedVector localDecodedVector(execCtx_);
  const SelectivityVector& rows = GetParam().rows;
  // 9. One of the middle wraps adds nulls but 'canPeelsHaveNulls' is set to
  //    false.
  //    Input Vectors: DictNoNulls(DictWithNulls(Flat1)), Const1,
  //                   DictNoNulls(DictWithNulls((Flat2))
  //    Peeled Vectors: DictWithNulls(Flat1), Const1,
  //                    DictWithNulls(Dict3(Flat2))
  //    Peel: DictNoNulls
  DictionaryWrap dictNoNulls{
      .indices = dictWrap1.indices, .nulls = nullptr, .size = dictWrap3.size};
  auto dictWithNulls = dictWrap2;
  auto input1 = wrapInDictionaryLayers(flat1, {&dictWithNulls, &dictNoNulls});
  auto input2 = const1;
  auto input3 =
      wrapInDictionaryLayers(flat2, {&dictWrap3, &dictWithNulls, &dictNoNulls});
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding = PeeledEncoding::peel(
      {input1, input2, input3}, rows, localDecodedVector, false, peeledVectors);
  ASSERT_EQ(peeledVectors.size(), 3);
  ASSERT_EQ(peeledEncoding->wrapEncoding(), VectorEncoding::Simple::DICTIONARY);
  ASSERT_EQ(peeledVectors[0].get(), peelWrappings(1, input1).get());
  if (peeledVectors[1].get() != const1.get()) {
    // In case the constant is resized to match other peeledVector's size.
    assertEqualVectors(peeledVectors[1], const1, rows);
  }
  ASSERT_EQ(peeledVectors[2].get(), peelWrappings(1, input3).get());
  assertEqualVectors(
      wrapInDictionaryLayers(flat1, {&dictNoNulls}),
      peeledEncoding->wrap(flat1->type(), pool(), flat1, rows),
      rows);
}

TEST_P(PeeledEncodingBasicTests, constantResize) {
  LocalDecodedVector localDecodedVector(execCtx_);
  const SelectivityVector& rows = GetParam().rows;
  // 10. Verify that the constant vector is resized to at least the same length
  //    as the base vector after peeling dictionary layers.
  //    Input Vectors: Dict1(Dict2(FlatLarge)), ConstSmallerSize,
  //    Peeled Vectors: FlatLarge, ConstSizeSameAsFlatLarge
  //    Peel: Dict1(Dict2) => collapsed into one dictionary
  auto flatLarge = fuzzer->fuzzFlat(INTEGER(), 2 * vectorSize_);
  auto input1 = wrapInDictionaryLayers(flatLarge, {&dictWrap2, &dictWrap1});
  auto input2 = const1;
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding = PeeledEncoding::peel(
      {input1, input2}, rows, localDecodedVector, true, peeledVectors);
  ASSERT_EQ(peeledVectors.size(), 2);
  ASSERT_EQ(peeledEncoding->wrapEncoding(), VectorEncoding::Simple::DICTIONARY);
  ASSERT_EQ(peeledVectors[0].get(), flatLarge.get());
  ASSERT_NE(peeledVectors[1].get(), const1.get());
  ASSERT_EQ(peeledVectors[1]->encoding(), VectorEncoding::Simple::CONSTANT);
  ASSERT_EQ(peeledVectors[1]->size(), 2 * vectorSize_);
  assertEqualVectors(
      input1,
      peeledEncoding->wrap(flatLarge->type(), pool(), flatLarge, rows),
      rows);
}

TEST_P(PeeledEncodingBasicTests, intermidiateLazyLayer) {
  LocalDecodedVector localDecodedVector(execCtx_);
  const SelectivityVector& rows = GetParam().rows;
  // 11. Ensure peeling also removes a loaded lazy layer.
  //    Input Vectors: Dict1(Lazy(Dict2(Flat1)))
  //    Peeled Vectors: Flat1
  //    Peel: Dict1(Dict2) => collapsed into one dictionary

  auto input1 = fuzzer->wrapInLazyVector(
      wrapInDictionaryLayers(flat1, {&dictWrap2, &dictWrap1}));
  // Make sure its loaded so that peeling will go past the lazy layer.
  input1->loadedVector();
  std::vector<VectorPtr> peeledVectors;
  auto peeledEncoding = PeeledEncoding::peel(
      {input1}, rows, localDecodedVector, true, peeledVectors);
  ASSERT_EQ(peeledVectors.size(), 1);
  ASSERT_EQ(peeledEncoding->wrapEncoding(), VectorEncoding::Simple::DICTIONARY);
  ASSERT_TRUE(peeledVectors[0]->isFlatEncoding());
  // Loading generates a new vector so we compare their contents instead.
  LocalSelectivityVector traslatedRowsHolder(execCtx_);
  auto translatedRows =
      peeledEncoding->translateToInnerRows(rows, traslatedRowsHolder);
  assertEqualVectors(peeledVectors[0], flat1, *translatedRows);
}

TEST_F(PeeledEncodingTest, peelingFails) {
  VectorFuzzer::Options options;
  options.nullRatio = 0.3;
  VectorFuzzer fuzzer(options, pool());
  vector_size_t size = 100;

  auto dictWrap1 = generateDictionaryWrap(fuzzer, size);
  auto dictWrap2 = generateDictionaryWrap(fuzzer, size);

  auto flat1 = fuzzer.fuzzFlat(INTEGER());
  auto flat2 = fuzzer.fuzzFlat(INTEGER());
  auto const1 = fuzzer.fuzzConstant(INTEGER());

  SelectivityVector rows(size);
  LocalDecodedVector localDecodedVector(execCtx_);

  // 1. Top level dictionary layers are different
  //    Input Vectors: Dict1(Flat1), Dict2(Flat2)
  {
    auto input1 = wrapInDictionaryLayers(flat1, {&dictWrap1});
    auto input2 = wrapInDictionaryLayers(flat2, {&dictWrap2});
    std::vector<VectorPtr> peeledVectors;
    auto peeledEncoding = PeeledEncoding::peel(
        {input1, input2}, rows, localDecodedVector, true, peeledVectors);
    ASSERT_TRUE(peeledVectors.empty());
    ASSERT_TRUE(!peeledEncoding);
  }

  // 2. One of the inputs does not have any wraps.
  //    Input Vectors: Dict1(Dict2(Flat1)), Dict1(Const1)), Flat2
  {
    auto input1 = wrapInDictionaryLayers(flat1, {&dictWrap2, &dictWrap1});
    auto input2 = wrapInDictionaryLayers(const1, {&dictWrap1});
    auto input3 = flat2;
    std::vector<VectorPtr> peeledVectors;
    auto peeledEncoding = PeeledEncoding::peel(
        {input1, input2, input3},
        rows,
        localDecodedVector,
        true,
        peeledVectors);
    ASSERT_TRUE(peeledVectors.empty());
    ASSERT_TRUE(!peeledEncoding);
  }

  // 3. Topmost wrap adds nulls but 'canPeelsHaveNulls' is set to false.
  //    Input Vectors: DictWithNulls(Flat1)), Const1,
  //                   DictWithNulls(Dict2((Flat2))
  {
    auto dictWithNulls = dictWrap1;
    auto input1 = wrapInDictionaryLayers(flat1, {&dictWithNulls});
    auto input2 = const1;
    auto input3 = wrapInDictionaryLayers(flat2, {&dictWrap2, &dictWithNulls});
    std::vector<VectorPtr> peeledVectors;
    auto peeledEncoding = PeeledEncoding::peel(
        {input1, input2, input3},
        rows,
        localDecodedVector,
        false,
        peeledVectors);
    ASSERT_TRUE(peeledVectors.empty());
    ASSERT_TRUE(!peeledEncoding);
  }
}
