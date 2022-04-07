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
namespace facebook {
namespace velox {

template <typename T>
class Tree;

template <typename T>
class TreeIterator {
 public:
  typedef TreeIterator<T> self_type;
  typedef const T value_type;
  typedef const T& reference;
  typedef const T* pointer;
  // todo: support more iterator types
  typedef std::forward_iterator_tag iterator_category;
  typedef int64_t difference_type;

  TreeIterator(const Tree<T>& tree, size_t idx) : tree_(tree), idx_{idx} {}

  self_type operator++() {
    // prefix
    idx_++;
    return *this;
  }

  self_type operator++(int32_t) {
    // postfix
    self_type i = *this;
    idx_++;
    return i;
  }

  reference operator*() {
    return tree_.childAt(idx_);
  }

  pointer operator->() {
    return &tree_.childAt(idx_);
  }

  bool operator==(const self_type& rhs) const {
    return &tree_ == &rhs.tree_ && idx_ == rhs.idx_;
  }

  bool operator!=(const self_type& rhs) const {
    return (&tree_ != &(rhs.tree_)) || idx_ != rhs.idx_;
  }

 private:
  const Tree<T>& tree_;
  size_t idx_;
};

template <typename T>
class Tree {
 public:
  using const_reference = const T&;
  using reference_type = const_reference;
  using const_iterator = TreeIterator<T>;
  using iterator = const_iterator;

  using difference_type = uint64_t;
  using size_type = uint32_t;

  virtual ~Tree() = default;

  virtual size_type size() const = 0;

  virtual const_reference childAt(size_type idx) const = 0;

  const_iterator begin() const {
    return cbegin();
  };

  const_iterator end() const {
    return cend();
  };

  const_iterator cbegin() const {
    return const_iterator{*this, 0};
  }

  const_iterator cend() const {
    return const_iterator{*this, size()};
  }

  // todo(youknowjack): remaining container members
};

} // namespace velox
} // namespace facebook
