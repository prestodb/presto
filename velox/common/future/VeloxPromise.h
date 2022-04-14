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

#include <folly/futures/Future.h>

namespace facebook::velox {
/// Simple wrapper around folly's promise to track down destruction of
/// unfulfilled promises.
template <class T>
class VeloxPromise : public folly::Promise<T> {
 public:
  VeloxPromise() : folly::Promise<T>() {}

  explicit VeloxPromise(const std::string& context)
      : folly::Promise<T>(), context_(context) {}

  VeloxPromise(
      folly::futures::detail::EmptyConstruct,
      const std::string& context) noexcept
      : folly::Promise<T>(folly::Promise<T>::makeEmpty()), context_(context) {}

  ~VeloxPromise() {
    if (!this->isFulfilled()) {
      LOG(WARNING) << "PROMISE: Unfulfilled promise is being deleted. Context: "
                   << context_;
    }
  }

  explicit VeloxPromise(VeloxPromise<T>&& other)
      : folly::Promise<T>(std::move(other)),
        context_(std::move(other.context_)) {}

  VeloxPromise& operator=(VeloxPromise<T>&& other) noexcept {
    folly::Promise<T>::operator=(std::move(other));
    context_ = std::move(other.context_);
    return *this;
  }

  static VeloxPromise makeEmpty(const std::string& context = "") noexcept {
    return VeloxPromise<T>(folly::futures::detail::EmptyConstruct{}, context);
  }

 private:
  /// Optional parameter to understand where this promise was created.
  std::string context_;
};

/// Equivalent of folly's makePromiseContract for VeloxPromise.
template <class T>
std::pair<VeloxPromise<T>, folly::SemiFuture<T>> makeVeloxPromiseContract(
    const std::string& promiseContext = "") {
  auto p = VeloxPromise<T>(promiseContext);
  auto f = p.getSemiFuture();
  return std::make_pair(std::move(p), std::move(f));
}

using ContinuePromise = VeloxPromise<bool>;

} // namespace facebook::velox
