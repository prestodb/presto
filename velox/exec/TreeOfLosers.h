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

#include <memory>
#include <optional>
#include <vector>

namespace facebook::velox {

template <typename Value, typename Source>
class TreeOfLosers {
 public:
  class Node {
   public:
    Node(std::unique_ptr<Source>&& source) : source_(std::move(source)) {}
    Node(std::unique_ptr<Node> left, std::unique_ptr<Node>&& right)
        : left_(std::move(left)), right_(std::move(right)) {}

    template <typename Compare>
    std::optional<Value> front(const Compare& compare) {
      if (atEnd_) {
        return std::nullopt;
      }
      if (value_.has_value()) {
        return value_;
      }
      if (source_) {
        if (source_->atEnd()) {
          atEnd_ = true;
          return std::nullopt;
        }
        value_ = source_->next();
        return std::optional<Value>(value_);
      }
      auto leftValue = left_->front(compare);
      auto rightValue = right_->front(compare);
      if (!leftValue.has_value()) {
        if (!rightValue.has_value()) {
          atEnd_ = true;
          return std::nullopt;
        }
        value_ = rightValue;
        right_->pop();
        return value_;
      }
      if (!rightValue.has_value()) {
        value_ = leftValue;
        left_->pop();
        return value_;
      }
      int32_t result = compare(leftValue.value(), rightValue.value());
      if (result <= 0) {
        value_ = leftValue;
        left_->pop();
      } else {
        value_ = rightValue;
        right_->pop();
      }
      return value_;
    }

    void pop() {
      value_ = std::nullopt;
    }

    std::optional<Value> value_;
    bool atEnd_ = false;
    std::unique_ptr<Node> left_;
    std::unique_ptr<Node> right_;
    std::unique_ptr<Source> source_;
  };

  TreeOfLosers(std::vector<std::unique_ptr<Source>>&& sources) {
    std::vector<std::unique_ptr<Node>> level(sources.size());
    for (auto i = 0; i < sources.size(); ++i) {
      level[i] = std::make_unique<Node>(std::move(sources[i]));
    }
    while (level.size() > 1) {
      std::vector<std::unique_ptr<Node>> nextLevel;
      for (auto i = 0; i < level.size(); i += 2) {
        if (i <= level.size() - 2) {
          nextLevel.push_back(std::make_unique<Node>(
              std::move(level[i]), std::move(level[i + 1])));
        } else {
          nextLevel.push_back(std::move(level[i]));
        }
      }
      level = std::move(nextLevel);
    }
    root_ = std::move(level[0]);
  }

  template <typename Compare>
  std::optional<Value> next(const Compare& compare) {
    auto value = root_->front(compare);
    root_->pop();
    return value;
  }

 private:
  std::unique_ptr<Node> root_;
};
} // namespace facebook::velox
