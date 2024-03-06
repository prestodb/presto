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

#include <functional>
#include <variant>
#include <vector>

#include "folly/Range.h"
#include "folly/Unit.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::velox::dwio::common {

// Use this class if you want to return a result or a set of actions needed
// before achieving that result.
// For example, if IO needs to be performed, instead of blocking and doing the
// IO on the same thread, the reader can return the NEEDS_MORE_IO state and a
// callback, or a set of callbacks, if parallelizable, to perform the IO.
// Some states may not require actions.
// If there's a result, no action is expected.
//
// Use ResultOrActions<folly::Unit> if you don't need a result (instead of
// ResultOrActions<void>, which isn't supported).
template <typename ResultType, typename ActionSignature = void()>
class ResultOrActions {
 public:
  // This constructor doesn't exist if we don't have a result
  // (ResultType=folly::Unit)
  template <
      typename T = ResultType,
      typename std::enable_if_t<!std::is_same_v<T, folly::Unit>, int> = 0>
  /* implicit */ ResultOrActions(std::conditional_t<true, ResultType, T> result)
      : resultOrActions_(std::move(result)) {}

  /* implicit */ ResultOrActions(std::function<ActionSignature> action)
      : resultOrActions_{
            std::vector<std::function<ActionSignature>>{{std::move(action)}}} {}

  /* implicit */ ResultOrActions(
      std::vector<std::function<ActionSignature>> actions = {})
      : resultOrActions_{std::move(actions)} {}

  /* implicit */ ResultOrActions(ResultOrActions&& other) noexcept
      : resultOrActions_{std::move(other.resultOrActions_)} {}

  ResultOrActions& operator=(ResultOrActions&& other) {
    resultOrActions_ = std::move(other.resultOrActions_);
    return *this;
  }

  template <typename T = ResultType>
  typename std::enable_if_t<!std::is_same_v<T, folly::Unit>, bool> hasResult()
      const {
    return resultOrActions_.index() == 1;
  }

  template <typename T = ResultType>
  const typename std::enable_if_t<!std::is_same_v<T, folly::Unit>, ResultType>&
  result() const {
    VELOX_CHECK(hasResult(), "Result is not set");
    return std::get<ResultType>(resultOrActions_);
  }

  template <typename T = ResultType>
  typename std::enable_if_t<!std::is_same_v<T, folly::Unit>, ResultType>&
  result() {
    VELOX_CHECK(hasResult(), "Result is not set");
    return std::get<ResultType>(resultOrActions_);
  }

  folly::Range<std::function<ActionSignature>*> actions() {
    switch (resultOrActions_.index()) {
      case 0: // Actions
        return {
            std::get<std::vector<std::function<ActionSignature>>>(
                resultOrActions_)
                .data(),
            std::get<std::vector<std::function<ActionSignature>>>(
                resultOrActions_)
                .size()};
      case 1: // Result
        return {};
      default:
        VELOX_FAIL("Unexpected variant index");
    }
  }

  size_t runAllActions() {
    size_t numActions = 0;
    switch (resultOrActions_.index()) {
      case 0: // Actions
      {
        auto& actions = std::get<std::vector<std::function<ActionSignature>>>(
            resultOrActions_);
        numActions = actions.size();
        for (auto& action : actions) {
          action();
        }
        break;
      }
      default:
        break;
    }
    return numActions;
  }

 private:
  // Otherwise mergeActionsFrom can't read other's private members, since
  // they're different classes
  template <typename OtherResultType, typename OtherActionSignature>
  friend class ResultOrActions;

  void mergeAction(std::function<ActionSignature> action) {
    switch (resultOrActions_.index()) {
      case 0: // Actions
        std::get<std::vector<std::function<ActionSignature>>>(resultOrActions_)
            .push_back(std::move(action));
        break;
      case 1: // Result
        VELOX_FAIL("Can't merge actions if destination class has a result");
    }
  }

  std::variant<std::vector<std::function<ActionSignature>>, ResultType>
      resultOrActions_;
};

} // namespace facebook::velox::dwio::common
