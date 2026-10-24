/*
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

#include "presto_cpp/main/tvf/spi/TableFunction.h"

using namespace facebook::velox;

namespace facebook::presto::tvf {

class SimpleTableFunctionHandle : public TableFunctionHandle {
 public:
  std::string_view name() const override {
    return "SimpleTableFunctionHandle";
  };

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    return obj;
  };

  static std::shared_ptr<SimpleTableFunctionHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::shared_ptr<SimpleTableFunctionHandle>();
  };

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("SimpleTableFunctionHandle", create);
  }
};

class SimpleTableFunctionAnalysis : public TableFunctionAnalysis {};

class SimpleTableFunction final : public TableFunction {
 public:
  static std::unique_ptr<SimpleTableFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);
};

void registerSimpleTableFunction(const std::string& name);

class IdentityFunctionHandle : public TableFunctionHandle {
  std::string_view name() const override {
    return "IdentityFunctionHandle";
  };

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    return obj;
  };

  static std::shared_ptr<IdentityFunctionHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::shared_ptr<IdentityFunctionHandle>();
  };

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("IdentityFunctionHandle", create);
  }
};

class IdentityFunctionAnalysis : public TableFunctionAnalysis {};

class IdentityDataProcessor : public TableFunctionDataProcessor {
 public:
  explicit IdentityDataProcessor(
      const IdentityFunctionHandle* handle,
      memory::MemoryPool* pool)
      : TableFunctionDataProcessor("identity", pool, nullptr),
        handle_(handle) {}

  std::shared_ptr<TableFunctionResult> apply(
      const std::vector<velox::RowVectorPtr>& input) override;

 private:
  const IdentityFunctionHandle* handle_;
};

class IdentityFunction final : public TableFunction {
 public:
  static std::unique_ptr<IdentityFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);
};

void registerIdentityFunction(const std::string& name);

class RepeatFunctionHandle : public TableFunctionHandle {
 public:
  explicit RepeatFunctionHandle(int64_t count) : count_(count) {}

  std::string_view name() const override {
    return "RepeatFunctionHandle";
  };

  folly::dynamic serialize() const override {
    folly::dynamic obj = folly::dynamic::object;
    obj["name"] = fmt::format("{}", name());
    obj["count"] = count_;
    return obj;
  };

  static std::shared_ptr<RepeatFunctionHandle> create(
      const folly::dynamic& obj,
      void* context) {
    return std::make_shared<RepeatFunctionHandle>(obj["count"].asInt());
  };

  static void registerSerDe() {
    auto& registry = velox::DeserializationWithContextRegistryForSharedPtr();
    registry.Register("RepeatFunctionHandle", create);
  }

  int64_t count() const {
    return count_;
  }

 private:
  int64_t count_;
};

class RepeatFunctionDataProcessor : public TableFunctionDataProcessor {
 public:
  RepeatFunctionDataProcessor(
      const RepeatFunctionHandle* handle,
      velox::memory::MemoryPool* pool)
      : TableFunctionDataProcessor("repeat", pool, nullptr), handle_(handle) {}

  std::shared_ptr<TableFunctionResult> apply(
      const std::vector<velox::RowVectorPtr>& input) override;

 private:
  const RepeatFunctionHandle* handle_;
};

class RepeatFunctionAnalysis : public TableFunctionAnalysis {};

class RepeatFunction final : public TableFunction {
 public:
  static std::unique_ptr<RepeatFunctionAnalysis> analyze(
      const std::unordered_map<std::string, std::shared_ptr<Argument>>& args);
};

void registerRepeatFunction(const std::string& name);
} // namespace facebook::presto::tvf
