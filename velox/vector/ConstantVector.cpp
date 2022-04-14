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

#include "velox/vector/ConstantVector.h"

namespace facebook {
namespace velox {

template <>
void ConstantVector<StringView>::setValue(const std::string& string) {
  if (string.size() <= StringView::kInlineSize) {
    value_ = StringView(string);
    return;
  }
  stringBuffer_ = AlignedBuffer::allocate<char>(string.size(), pool());
  memcpy(stringBuffer_->asMutable<char>(), string.data(), string.size());
  value_ = StringView(stringBuffer_->as<char>(), stringBuffer_->size());
}

template <>
void ConstantVector<std::shared_ptr<void>>::setValue(
    const std::string& /*string*/) {
  VELOX_NYI();
}

template <>
void ConstantVector<ComplexType>::setValue(const std::string& /*string*/) {
  VELOX_UNSUPPORTED(
      "ConstantVectors of ComplexType cannot be initialized from string values.");
}

} // namespace velox
} // namespace facebook
