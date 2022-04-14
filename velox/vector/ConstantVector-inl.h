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

namespace facebook {
namespace velox {

template <typename T>
std::unique_ptr<SimpleVector<uint64_t>> ConstantVector<T>::hashAll() const {
  return std::make_unique<ConstantVector<uint64_t>>(
      BaseVector::pool_,
      BaseVector::length_,
      false /* isNull */,
      this->hashValueAt(0),
      SimpleVectorStats<uint64_t>{}, /* stats */
      sizeof(uint64_t) * BaseVector::length_ /* representedBytes */);
}

} // namespace velox
} // namespace facebook
