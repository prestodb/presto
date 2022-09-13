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

#include <mutex>
#include <typeindex>
#include "folly/Optional.h"
#include "folly/SharedMutex.h"
#include "folly/concurrency/ConcurrentHashMap.h"
#include "set"
#include "unordered_map"
#include "velox/common/base/Exceptions.h"

namespace facebook {
namespace velox {

enum class UseCase {
  DEV = 1,
  TEST = 2,
  PROD = 3,
};

enum class ContextScope { GLOBAL, SESSION, QUERY, SCOPESTACK };

class Config {
 public:
  virtual folly::Optional<std::string> get(const std::string& key) const = 0;
  // virtual const string operator[](const std::string& key) = 0;
  // overload and disable not supported cases.

  template <typename T>
  folly::Optional<T> get(const std::string& key) const {
    auto val = get(key);
    if (val.hasValue()) {
      return folly::to<T>(val.value());
    } else {
      return folly::none;
    }
  }

  template <typename T>
  T get(const std::string& key, const T& defaultValue) const {
    auto val = get(key);
    if (val.hasValue()) {
      return folly::to<T>(val.value());
    } else {
      return defaultValue;
    }
  }

  virtual bool isValueExists(const std::string& key) const = 0;

  virtual const std::unordered_map<std::string, std::string>& values() const {
    VELOX_UNSUPPORTED("method values() is not supported by this config");
  }

  virtual ~Config() = default;
};

namespace core {

// This represents environment variables.
class GlobalConfig : public Config {
 public:
  folly::Optional<std::string> get(const std::string& key) const override;

  bool isValueExists(const std::string& key) const override;
};

class MemConfig : public Config {
 public:
  explicit MemConfig(const std::unordered_map<std::string, std::string>& values)
      : values_(values) {}

  explicit MemConfig() : values_{} {}

  explicit MemConfig(std::unordered_map<std::string, std::string>&& values)
      : values_(std::move(values)) {}

  folly::Optional<std::string> get(const std::string& key) const override;

  bool isValueExists(const std::string& key) const override;

  const std::unordered_map<std::string, std::string>& values() const override {
    return values_;
  }

 private:
  std::unordered_map<std::string, std::string> values_;
};

class ConfigStack : public Config {
 public:
  explicit ConfigStack(
      const std::vector<std::shared_ptr<const Config>>& configs)
      : configs_(configs) {
    for (const auto& config : configs_) {
      VELOX_USER_CHECK_NOT_NULL(config);
    }
  }

  explicit ConfigStack(std::vector<std::shared_ptr<const Config>>&& configs)
      : configs_(std::move(configs)) {
    for (const auto& config : configs_) {
      VELOX_USER_CHECK_NOT_NULL(config);
    }
  }

  folly::Optional<std::string> get(const std::string& key) const override;

  bool isValueExists(const std::string& key) const override;

  std::shared_ptr<const ConfigStack> stack(
      std::shared_ptr<const Config>& config) const&;

  std::shared_ptr<const ConfigStack> stack(
      std::shared_ptr<const Config>& config) &&;

 private:
  std::vector<std::shared_ptr<const Config>> configs_;
};

class ConfigStackHelper {
 public:
  static std::shared_ptr<const ConfigStack> stack(
      std::shared_ptr<const Config>& config1,
      std::shared_ptr<const Config>& config2);
};

template <class T>
class ISubscriber {
 public:
  virtual void update(const std::shared_ptr<const T>& newValue) = 0;
  virtual ~ISubscriber() = default;
};

// This class represents a configuration manager. It is intended to be used with
// a single producer and multiple consumers.
class BaseConfigManager {
 public:
  using TSubscription = std::function<void(const BaseConfigManager*)>;
  using TSubscriptionptr = std::shared_ptr<const TSubscription>;
  struct SubscriptionHandle {
    std::type_index obj;
    TSubscriptionptr ptr;
  };

  // TODO (bharathb) : Either disallow or flatten config stacks here.
  void setConfigOverrides(
      const std::shared_ptr<const Config>& configOverrides) {
    // TODO (bharathb) : this need to be atomic.
    std::atomic_exchange(&config_, configOverrides);
    updateSubscriptions();
  }

  void setConfigOverrides(
      std::unordered_map<std::string, std::string>&& configOverrides) {
    setConfigOverrides(
        std::make_shared<const MemConfig>(std::move(configOverrides)));
  }

  // template<class T> set(shared_ptr<const T>& obj) {}

  // TODO (bharathb) : replace dynamic_pointer_cast with
  // static_pointer_cast.
  template <class T>
  std::shared_ptr<const T> get() const {
    auto obj = std::make_shared<const T>(getConfigInternal());
    return obj;
  }

  template <typename T>
  folly::Optional<T> get(const std::string& key) const {
    return getConfigInternal()->get<T>(key);
  }

  template <typename T>
  T get(const std::string& key, const T& defaultValue) const {
    return getConfigInternal()->get<T>(key, defaultValue);
  }

  template <class T, class U>
  SubscriptionHandle subscribe(const std::shared_ptr<U>& subscriber) const {
    VELOX_CHECK_NOT_NULL(
        subscriber, "subscribe expects a non null object pointer");
    TSubscriptionptr subscription = std::make_shared<const TSubscription>(
        [subscriber](const BaseConfigManager* mgr) {
          subscriber->update(mgr->get<T>());
        });
    return AddSubscription<T>(subscription);
  }

  template <class T>
  SubscriptionHandle subscribe(
      const std::function<void(const std::shared_ptr<const T>)>& subscriber)
      const {
    TSubscriptionptr subscription = std::make_shared<const TSubscription>(
        [subscriber](const BaseConfigManager* mgr) {
          subscriber(mgr->get<T>());
        });
    return AddSubscription<T>(subscription);
  }

  template <class T>
  SubscriptionHandle subscribe(
      const std::shared_ptr<ISubscriber<T>>& subscriber) const {
    VELOX_CHECK_NOT_NULL(
        subscriber, "subscribe expects a non null object pointer");
    TSubscriptionptr subscription = std::make_shared<const TSubscription>(
        [subscriber](const BaseConfigManager* mgr) {
          subscriber->update(mgr->get<T>());
        });
    return AddSubscription<T>(subscription);
  }

  void unsubscribe(SubscriptionHandle& handle) {
    folly::SharedMutex::WriteHolder wm(mutex_);
    if (subscriptions_.find(handle.obj) == subscriptions_.end()) {
      return;
    }
    if (subscriptions_[handle.obj].erase(handle.ptr)) {
      handle.ptr.reset();
    }
  }

  // virtual shared_ptr<const IContext> getParent() = 0;
  virtual ~BaseConfigManager() = default;

 protected:
  std::shared_ptr<const Config> getConfig() const {
    return config_;
  }

  void updateSubscriptions() {
    folly::SharedMutex::ReadHolder rm(mutex_);
    for (auto& subscriptionPair : subscriptions_) {
      for (auto& subscription : subscriptionPair.second) {
        (*subscription)(this);
      }
    }
  }

  virtual std::shared_ptr<const Config> getConfigInternal() const {
    return getConfig();
  }

 private:
  template <class T>
  SubscriptionHandle AddSubscription(TSubscriptionptr& subscription) const {
    folly::SharedMutex::WriteHolder wm(mutex_);
    subscriptions_[typeid(T)].insert(subscription);
    return SubscriptionHandle{typeid(T), subscription};
  }

  //  std::atomic<shared_ptr<const Config>> config_;
  std::shared_ptr<const Config> config_;

  mutable folly::SharedMutex mutex_;
  // TODO (bharathb): Look for a lock free datastructure(replace with a list).
  // TODO (bharathb) : We notify everyone irrespective of whether their
  // registered object changed or not.
  mutable std::unordered_map<std::type_index, std::set<TSubscriptionptr>>
      subscriptions_;
};

class RawConfigUpdate {
 public:
  explicit RawConfigUpdate(const std::shared_ptr<const Config> config)
      : config_(config) {}
  std::shared_ptr<const Config> getConfig() const& {
    return config_;
  }

  const std::shared_ptr<const Config>&& getConfig() && {
    return std::move(config_);
  }

 private:
  std::shared_ptr<const Config> config_;
};

class Context : public BaseConfigManager {
 public:
  explicit Context(
      ContextScope scope,
      const std::shared_ptr<const Context> parent = nullptr);

  std::shared_ptr<const Config> getConfigInternal() const override;

  ContextScope scope() const {
    return scope_;
  }
  const std::shared_ptr<const Context>& parent() const {
    return parent_;
  }

 private:
  ContextScope scope_;
  const std::shared_ptr<const Context> parent_;
};

} // namespace core
} // namespace velox
} // namespace facebook
