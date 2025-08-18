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
#include "velox/common/file/TokenProvider.h"

namespace facebook::velox::filesystems {

class PlainUserNameAccessToken : public filesystems::AccessToken {
 public:
  explicit PlainUserNameAccessToken(const std::string& user) : user_(user) {}
  ~PlainUserNameAccessToken() override = default;
  std::string getUser() const {
    return user_;
  }

 private:
  std::string user_;
};

class PlainUserNameTokenProvider : public filesystems::TokenProvider {
 public:
  explicit PlainUserNameTokenProvider(const std::string& user) : user_(user) {}
  bool equals(const TokenProvider& other) const override {
    auto* o = dynamic_cast<const PlainUserNameTokenProvider*>(&other);
    return o && o->user_ == user_;
  }
  size_t hash() const override {
    return std::hash<std::string>()(user_);
  }
  std::shared_ptr<filesystems::AccessToken> getToken(
      const filesystems::AccessTokenKey& /*key*/) const override {
    return std::make_shared<PlainUserNameAccessToken>(user_);
  }

 private:
  std::string user_;
};

} // namespace facebook::velox::filesystems
