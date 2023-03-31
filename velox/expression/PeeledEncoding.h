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

#pragma once

#include "velox/vector/BaseVector.h"

namespace facebook::velox {
class DecodedVector;
}

namespace facebook::velox::exec {
class LocalDecodedVector;
class LocalSelectivityVector;

/// Utility class for enabling peeling functionality. It takes a list of
/// vectors, peels off the common encodings off of them, stores the peel and
/// returns a list of the peeled vectors. The saved peel can then be used to
/// translate top level rows into inner rows and wrap a vector (typically
/// generated as a result of applying an expression on the peeled vectors).
///
/// Typical usage pattern for peeling includes:
/// (See Expr::applyFunctionWithPeeling() for example usage)
/// 1. peeling a set of input vectors
/// 2. converting relevant rows (top level rows and final selection rows)
/// 3. Saving the current context and setting the peel
/// 4. Applying the function or moving forward with expression eval
/// 5. wrapping the result vector with the peel
///
/// Few important examples of how vectors are peeled:
/// 1. Common dictionary layers are peeled
///    Input Vectors: Dict1(Dict2(Flat1)), Dict1(Dict2(Const1)),
///                   Dict1(Dict2(Dict3(Flat2)))
///    Peeled Vectors: Flat, Const1, Dict3(Flat2)
///    Peel: Dict1(Dict2) => collapsed into one dictionary
///
/// 2. Common dictionary layers are peeled
///    Input Vectors: Dict1(Dict2(Flat1)), Dict1(Const1)),
///                   Dict1(Dict2(Dict3(Flat2)))
///    Peeled Vectors: Dict2(Flat), Const1, Dict2(Dict3(Flat2))
///    Peel: Dict1
///
/// 3. Common dictionary layers are peeled while constant is ignored
///    (since all valid rows translated via the common dictionary layers would
///    point to the same constant index)
///    Input Vectors: Dict1(Dict2(Flat1)), Const1,
///                   Dict1(Dict2(Dict3(Flat2)))
///    Peeled Vectors: Flat, Const1, Dict3(Flat2)
///    Peel: Dict1(Dict2) => collapsed into one dictionary
///
/// 4. A single vector with constant encoding layer over a complex vector can
///    be peeled.
///    Input Vectors: Const1(Complex1)
///    Peeled Vectors: Complex
///    Peel: Const1
///
/// 5. A single vector with arbitrary dictionary layers over a constant
///    encoding layer over a NULL complex vector can be peeled and the peels can
///    be merged into a single constant layer.
///    Input Vectors: Dict1(Const1(NullComplex))
///    Peeled Vectors: Null Complex vector
///    Peel: Const(generic constant index)
///
/// 6. All constant inputs where the peel is just a constant pointing to the
///    first valid row. The peel itself is irrelevant but allows us to translate
///    input rows into a single row.
///    Input Vectors: Const1(Complex1), Const2, Const3
///    Peeled Vectors: Const1(Complex1), Const2, Const3
///    Peel: Const(generic constant index)
///
/// 7. No inputs. This is considered as constant encoding since its expected
///    to produce the same result for all valid rows.
///    Input Vectors: <empty>
///    Peeled Vectors: <empty>
///    Peel: Const(generic constant index)
///
/// 8. A single vector with dictionary over constant encoding layer over a
///    complex vector can be peeled all the way to the complex vector.
///    Input Vectors: Dict1(Const1(Complex1))
///    Peeled Vectors: Complex
///    Peel: Dict1(Const1) => collapsed into one dictionary
///
/// 9. One of the middle wraps adds nulls but 'canPeelsHaveNulls' is set to
///    false.
///    Input Vectors: DictNoNulls(DictWithNulls(Flat1)), Const1,
///                   DictNoNulls(DictWithNulls((Flat2))
///    Peeled Vectors: DictWithNulls(Flat1), Const1,
///                    DictWithNulls(Dict3(Flat2))
///    Peel: DictNoNulls
class PeeledEncoding {
 public:
  /// Factory method for constructing a PeeledEncoding object only if peeling
  /// was successful. Takes a set of vectors and peels all the common encoding
  /// layers off of them. Returns the set of vectors obtained after peeling
  /// those layers. Ensures that all peeled vectors will have valid rows for
  /// every valid inner row (rows obtained by translating top level rows via the
  /// peels). Returns a nullptr if peeling was unsuccessful, otherwise returns a
  /// valid PeeledEncoding object and populates 'peeledVectors' with the peeled
  /// vectors.
  static std::shared_ptr<PeeledEncoding> Peel(
      const std::vector<VectorPtr>& vectorsToPeel,
      const SelectivityVector& rows,
      LocalDecodedVector& decodedVector,
      bool canPeelsHaveNulls,
      std::vector<VectorPtr>& peeledVectors);

  /// Utility method used to check whether an encoding is peel-able.
  constexpr static bool isPeelable(VectorEncoding::Simple encoding) {
    switch (encoding) {
      case VectorEncoding::Simple::CONSTANT:
      case VectorEncoding::Simple::DICTIONARY:
      case VectorEncoding::Simple::SEQUENCE:
        return true;
      default:
        return false;
    }
  }

  /// Return the encoding of the peeled wrap.
  VectorEncoding::Simple getWrapEncoding() const;

  /// Translates row numbers of the outer vector via the peel into row numbers
  /// of the inner vector. Returns a pointer to the selectivityVector
  /// (representing inner rows) allocated from the input "innerRowsHolder".
  SelectivityVector* translateToInnerRows(
      const SelectivityVector& outerRows,
      LocalSelectivityVector& innerRowsHolder) const;

  /// Wraps the given vector 'peeledResult' with the peeled encoding and
  /// generates a vector which has valid rows as per the input 'rows'
  /// selectivity vector.
  VectorPtr wrap(
      const TypePtr& outputType,
      velox::memory::MemoryPool* FOLLY_NONNULL pool,
      VectorPtr peeledResult,
      const SelectivityVector& rows) const;

  /// Takes a set of outer rows (rows defined over the peel) and a functor that
  /// is executed for each outer row which does not map to a null row.
  /// The outer and its corresponding inner row is passed to the functor.
  void applyToNonNullInnerRows(
      const SelectivityVector& outerRows,
      std::function<void(vector_size_t, vector_size_t)> func) const {
    auto indices = wrap_ ? wrap_->as<vector_size_t>() : nullptr;
    auto wrapNulls = wrapNulls_ ? wrapNulls_->as<uint64_t>() : nullptr;
    outerRows.applyToSelected([&](auto outerRow) {
      // A known null in the outer row masks an error.
      if (wrapNulls && bits::isBitNull(wrapNulls, outerRow)) {
        return;
      }
      vector_size_t innerRow = indices ? indices[outerRow] : constantWrapIndex_;
      func(outerRow, innerRow);
    });
  };

 private:
  PeeledEncoding() = default;

  // Contains the actual implementation of peeling. Return true is peeling was
  // successful.
  bool peelInternal(
      const std::vector<VectorPtr>& vectorsToPeel,
      const SelectivityVector& rows,
      LocalDecodedVector& decodedVector,
      bool canPeelsHaveNulls,
      std::vector<VectorPtr>& peeledVectors);

  void setDictionaryWrapping(
      DecodedVector& decoded,
      const SelectivityVector& rows,
      BaseVector& firstWrapper);

  /// The encoding of the peel. Set after getPeeledVectors() is called. Is equal
  /// to Flat if getPeeledVectors() has not been called or peeling was not
  /// successful.
  VectorEncoding::Simple wrapEncoding_ = VectorEncoding::Simple::FLAT;

  /// The dictionary indices. Only valid if wrapEncoding_ = DICTIONARY.
  BufferPtr wrap_;

  /// The dictionary nulls. Only valid if wrapEncoding_ = DICTIONARY.
  BufferPtr wrapNulls_;

  /// The size of one of the peeled vectors. Only valid if wrapEncoding_ =
  /// DICTIONARY.
  vector_size_t baseSize_ = 0;

  /// The constant index. Only valid if wrapEncoding_ = CONSTANT.
  vector_size_t constantWrapIndex_ = 0;
};
} // namespace facebook::velox::exec
