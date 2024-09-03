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

// Adapted from Apache Arrow.

#pragma once

#include <cstdint>
#include <string>

namespace facebook::velox::parquet {

struct ByteArray {
  ByteArray() : len(0), ptr(nullptr) {}
  ByteArray(uint32_t len, const uint8_t* ptr) : len(len), ptr(ptr) {}

  ByteArray(::std::string_view view) // NOLINT implicit conversion
      : ByteArray(
            static_cast<uint32_t>(view.size()),
            reinterpret_cast<const uint8_t*>(view.data())) {}

  explicit operator std::string_view() const {
    return std::string_view{reinterpret_cast<const char*>(ptr), len};
  }

  uint32_t len;
  const uint8_t* ptr;
};

// Abstract class for hash
class Hasher {
 public:
  /// Compute hash for 32 bits value by using its plain encoding result.
  ///
  /// @param value the value to hash.
  /// @return hash result.
  virtual uint64_t hash(int32_t value) const = 0;

  /// Compute hash for 64 bits value by using its plain encoding result.
  ///
  /// @param value the value to hash.
  /// @return hash result.
  virtual uint64_t hash(int64_t value) const = 0;

  /// Compute hash for float value by using its plain encoding result.
  ///
  /// @param value the value to hash.
  /// @return hash result.
  virtual uint64_t hash(float value) const = 0;

  /// Compute hash for double value by using its plain encoding result.
  ///
  /// @param value the value to hash.
  /// @return hash result.
  virtual uint64_t hash(double value) const = 0;

  /// Compute hash for ByteArray value by using its plain encoding result.
  ///
  /// @param value the value to hash.
  /// @return hash result.
  virtual uint64_t hash(const ByteArray* value) const = 0;

  /// Batch compute hashes for 32 bits values by using its plain encoding
  /// result.
  ///
  /// @param values a pointer to the values to hash.
  /// @param num_values the number of values to hash.
  /// @param hashes a pointer to the output hash values, its length should be
  /// equal to num_values.
  virtual void hashes(const int32_t* values, int num_values, uint64_t* hashes)
      const = 0;

  /// Batch compute hashes for 64 bits values by using its plain encoding
  /// result.
  ///
  /// @param values a pointer to the values to hash.
  /// @param num_values the number of values to hash.
  /// @param hashes a pointer to the output hash values, its length should be
  /// equal to num_values.
  virtual void hashes(const int64_t* values, int num_values, uint64_t* hashes)
      const = 0;

  /// Batch compute hashes for float values by using its plain encoding result.
  ///
  /// @param values a pointer to the values to hash.
  /// @param num_values the number of values to hash.
  /// @param hashes a pointer to the output hash values, its length should be
  /// equal to num_values.
  virtual void hashes(const float* values, int num_values, uint64_t* hashes)
      const = 0;

  /// Batch compute hashes for double values by using its plain encoding result.
  ///
  /// @param values a pointer to the values to hash.
  /// @param num_values the number of values to hash.
  /// @param hashes a pointer to the output hash values, its length should be
  /// equal to num_values.
  virtual void hashes(const double* values, int num_values, uint64_t* hashes)
      const = 0;

  /// Batch compute hashes for ByteArray values by using its plain encoding
  /// result.
  ///
  /// @param values a pointer to the values to hash.
  /// @param num_values the number of values to hash.
  /// @param hashes a pointer to the output hash values, its length should be
  /// equal to num_values.
  virtual void hashes(const ByteArray* values, int num_values, uint64_t* hashes)
      const = 0;

  virtual ~Hasher() = default;
};

} // namespace facebook::velox::parquet
