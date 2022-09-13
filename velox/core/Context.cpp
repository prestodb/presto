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
#include "Context.h"

namespace facebook {
namespace velox {
namespace core {
folly::Optional<std::string> GlobalConfig::get(const std::string& key) const {
  folly::Optional<std::string> val;
  if (char* strVal = std::getenv(key.c_str())) {
    val = strVal;
  }
  return val;
}

bool GlobalConfig::isValueExists(const std::string& key) const {
  return std::getenv(key.c_str()) != nullptr;
}

folly::Optional<std::string> MemConfig::get(const std::string& key) const {
  folly::Optional<std::string> val;
  auto it = values_.find(key);
  if (it != values_.end()) {
    val = it->second;
  }
  return val;
}

bool MemConfig::isValueExists(const std::string& key) const {
  return values_.find(key) != values_.end();
}

folly::Optional<std::string> ConfigStack::get(const std::string& key) const {
  folly::Optional<std::string> val;
  for (int64_t i = configs_.size(); --i >= 0;) {
    if (configs_[i]->isValueExists(key)) {
      return configs_[i]->get(key);
    }
  }
  return val;
}

bool ConfigStack::isValueExists(const std::string& key) const {
  for (int64_t i = configs_.size(); --i >= 0;) {
    if (configs_[i]->isValueExists(key)) {
      return true;
    }
  }
  return false;
}

std::shared_ptr<const ConfigStack> ConfigStack::stack(
    std::shared_ptr<const Config>& config) const& {
  if (!config || std::dynamic_pointer_cast<const ConfigStack>(config)) {
    throw std::invalid_argument(
        "Null Config or ConfigStack are not supported for stacking");
  }
  std::vector<std::shared_ptr<const Config>> configs(configs_);
  configs.push_back(config);
  return std::make_shared<const ConfigStack>(std::move(configs));
}

std::shared_ptr<const ConfigStack> ConfigStack::stack(
    std::shared_ptr<const Config>& config) && {
  if (!config || std::dynamic_pointer_cast<const ConfigStack>(config)) {
    throw std::invalid_argument(
        "Null Config or ConfigStack are not supported for stacking");
  }
  std::vector<std::shared_ptr<const Config>> configs(std::move(configs_));
  configs.push_back(config);
  return std::make_shared<const ConfigStack>(std::move(configs));
}

std::shared_ptr<const ConfigStack> ConfigStackHelper::stack(
    std::shared_ptr<const Config>& config1,
    std::shared_ptr<const Config>& config2) {
  VELOX_USER_CHECK_NOT_NULL(config1);
  VELOX_USER_CHECK_NOT_NULL(config2);
  if (auto configStack =
          std::dynamic_pointer_cast<const ConfigStack>(config1)) {
    return configStack->stack(config2);
  }
  std::vector<std::shared_ptr<const Config>> configs{config1, config2};
  return std::make_shared<const ConfigStack>(std::move(configs));
}

Context::Context(
    ContextScope scope,
    const std::shared_ptr<const Context> parent)
    : scope_(scope), parent_(parent) {
  if (parent_ != nullptr) {
    parent_->subscribe<RawConfigUpdate>(
        [this](const std::shared_ptr<const RawConfigUpdate> /* unused */) {
          auto config = getConfig();
          setConfigOverrides(config);
        });
  }
}

std::shared_ptr<const Config> Context::getConfigInternal() const {
  auto config = getConfig();
  if (LIKELY(parent_ != nullptr)) {
    auto parentConfig = parent_->getConfigInternal();
    return ConfigStackHelper::stack(parentConfig, config);
  }
  return config;
}

} // namespace core
} // namespace velox
} // namespace facebook
