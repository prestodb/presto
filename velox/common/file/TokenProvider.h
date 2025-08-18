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
#include <string>

namespace facebook::velox::filesystems {

/// Identifier for the file systems to implement to differentiate different
/// tokens needed in the same query (user).  Such information usually needs to
/// be passed down and stored in the ReadFile/WriteFile object of the specific
/// file system.
class AccessTokenKey {
 public:
  virtual ~AccessTokenKey() = default;
};

/// Abstract token each file system can implement and cast.
class AccessToken {
 public:
  virtual ~AccessToken() = default;
};

/// Interface for providing access tokens to file systems.
class TokenProvider {
 public:
  virtual ~TokenProvider() = default;

  virtual bool equals(const TokenProvider& other) const = 0;
  virtual size_t hash() const = 0;

  virtual std::shared_ptr<AccessToken> getToken(
      const AccessTokenKey& key) const = 0;
};

} // namespace facebook::velox::filesystems
