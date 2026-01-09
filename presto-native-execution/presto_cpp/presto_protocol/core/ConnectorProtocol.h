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

#include "presto_cpp/presto_protocol/core/presto_protocol_core.h"
#include "velox/common/base/Exceptions.h"

namespace facebook::presto::protocol {

class ConnectorProtocol;

void registerConnectorProtocol(
    const std::string& connectorName,
    std::unique_ptr<ConnectorProtocol> protocol);

void unregisterConnectorProtocol(const std::string& connectorName);

const ConnectorProtocol& getConnectorProtocol(const std::string& connectorName);

class ConnectorProtocol {
 public:
  virtual ~ConnectorProtocol() = default;

  virtual void to_json(json& j, const std::shared_ptr<ConnectorTableHandle>& p)
      const = 0;
  virtual void from_json(
      const json& j,
      std::shared_ptr<ConnectorTableHandle>& p) const = 0;
  virtual void serialize(
      const std::shared_ptr<ConnectorTableHandle>& proto,
      std::string& thrift) const = 0;
  virtual void deserialize(
      const std::string& thrift,
      std::shared_ptr<ConnectorTableHandle>& proto) const = 0;

  virtual void to_json(
      json& j,
      const std::shared_ptr<ConnectorTableLayoutHandle>& p) const = 0;
  virtual void from_json(
      const json& j,
      std::shared_ptr<ConnectorTableLayoutHandle>& p) const = 0;
  virtual void serialize(
      const std::shared_ptr<ConnectorTableLayoutHandle>& proto,
      std::string& thrift) const = 0;
  virtual void deserialize(
      const std::string& thrift,
      std::shared_ptr<ConnectorTableLayoutHandle>& proto) const = 0;

  virtual void to_json(json& j, const std::shared_ptr<ColumnHandle>& p)
      const = 0;
  virtual void from_json(const json& j, std::shared_ptr<ColumnHandle>& p)
      const = 0;

  virtual void to_json(
      json& j,
      const std::shared_ptr<ConnectorInsertTableHandle>& p) const = 0;
  virtual void from_json(
      const json& j,
      std::shared_ptr<ConnectorInsertTableHandle>& p) const = 0;
  virtual void serialize(
      const std::shared_ptr<ConnectorInsertTableHandle>& proto,
      std::string& thrift) const = 0;
  virtual void deserialize(
      const std::string& thrift,
      std::shared_ptr<ConnectorInsertTableHandle>& proto) const = 0;

  virtual void to_json(
      json& j,
      const std::shared_ptr<ConnectorDistributedProcedureHandle>& p) const = 0;
  virtual void from_json(
      const json& j,
      std::shared_ptr<ConnectorDistributedProcedureHandle>& p) const = 0;

  virtual void to_json(
      json& j,
      const std::shared_ptr<ConnectorOutputTableHandle>& p) const = 0;
  virtual void from_json(
      const json& j,
      std::shared_ptr<ConnectorOutputTableHandle>& p) const = 0;
  virtual void serialize(
      const std::shared_ptr<ConnectorOutputTableHandle>& proto,
      std::string& thrift) const = 0;
  virtual void deserialize(
      const std::string& thrift,
      std::shared_ptr<ConnectorOutputTableHandle>& proto) const = 0;

  virtual void to_json(json& j, const std::shared_ptr<ConnectorSplit>& p)
      const = 0;
  virtual void from_json(const json& j, std::shared_ptr<ConnectorSplit>& p)
      const = 0;
  virtual void serialize(
      const std::shared_ptr<ConnectorSplit>& proto,
      std::string& thrift) const = 0;
  virtual void deserialize(
      const std::string& thrift,
      std::shared_ptr<ConnectorSplit>& proto) const = 0;

  virtual void to_json(
      json& j,
      const std::shared_ptr<ConnectorPartitioningHandle>& p) const = 0;
  virtual void from_json(
      const json& j,
      std::shared_ptr<ConnectorPartitioningHandle>& p) const = 0;

  virtual void to_json(
      json& j,
      const std::shared_ptr<ConnectorTransactionHandle>& p) const = 0;
  virtual void from_json(
      const json& j,
      std::shared_ptr<ConnectorTransactionHandle>& p) const = 0;
  virtual void serialize(
      const std::shared_ptr<ConnectorTransactionHandle>& proto,
      std::string& thrift) const = 0;
  virtual void deserialize(
      const std::string& thrift,
      std::shared_ptr<ConnectorTransactionHandle>& proto) const = 0;

  virtual void to_json(
      json& j,
      const std::shared_ptr<ConnectorDeleteTableHandle>& p) const = 0;
  virtual void from_json(
      const json& j,
      std::shared_ptr<ConnectorDeleteTableHandle>& p) const = 0;
  virtual void serialize(
      const std::shared_ptr<ConnectorDeleteTableHandle>& proto,
      std::string& thrift) const = 0;
  virtual void deserialize(
      const std::string& thrift,
      std::shared_ptr<ConnectorDeleteTableHandle>& proto) const = 0;

  virtual void to_json(json& j, const std::shared_ptr<ConnectorIndexHandle>& p)
      const = 0;
  virtual void from_json(
      const json& j,
      std::shared_ptr<ConnectorIndexHandle>& p) const = 0;
};

namespace {
struct NotImplemented {};
} // namespace

template <
    typename ConnectorTableHandleType = NotImplemented,
    typename ConnectorTableLayoutHandleType = NotImplemented,
    typename ColumnHandleType = NotImplemented,
    typename ConnectorInsertTableHandleType = NotImplemented,
    typename ConnectorOutputTableHandleType = NotImplemented,
    typename ConnectorSplitType = NotImplemented,
    typename ConnectorPartitioningHandleType = NotImplemented,
    typename ConnectorTransactionHandleType = NotImplemented,
    typename ConnectorDistributedProcedureHandleType = NotImplemented,
    typename ConnectorDeleteTableHandleType = NotImplemented,
    typename ConnectorIndexHandleType = NotImplemented>
class ConnectorProtocolTemplate final : public ConnectorProtocol {
 public:
  void to_json(json& j, const std::shared_ptr<ConnectorTableHandle>& p)
      const final {
    to_json_template<ConnectorTableHandleType>(j, p);
  }
  void from_json(const json& j, std::shared_ptr<ConnectorTableHandle>& p)
      const final {
    from_json_template<ConnectorTableHandleType>(j, p);
  }
  void serialize(
      const std::shared_ptr<ConnectorTableHandle>& proto,
      std::string& thrift) const final {
    serializeTemplate<ConnectorTableHandleType>(proto, thrift);
  }
  void deserialize(
      const std::string& thrift,
      std::shared_ptr<ConnectorTableHandle>& proto) const final {
    deserializeTemplate<ConnectorTableHandleType>(thrift, proto);
  }

  void to_json(json& j, const std::shared_ptr<ConnectorTableLayoutHandle>& p)
      const final {
    to_json_template<ConnectorTableLayoutHandleType>(j, p);
  }
  void from_json(const json& j, std::shared_ptr<ConnectorTableLayoutHandle>& p)
      const final {
    from_json_template<ConnectorTableLayoutHandleType>(j, p);
  }
  void serialize(
      const std::shared_ptr<ConnectorTableLayoutHandle>& proto,
      std::string& thrift) const final {
    serializeTemplate<ConnectorTableLayoutHandleType>(proto, thrift);
  }
  void deserialize(
      const std::string& thrift,
      std::shared_ptr<ConnectorTableLayoutHandle>& proto) const final {
    deserializeTemplate<ConnectorTableLayoutHandleType>(thrift, proto);
  }

  void to_json(json& j, const std::shared_ptr<ColumnHandle>& p) const final {
    to_json_template<ColumnHandleType>(j, p);
  }
  void from_json(const json& j, std::shared_ptr<ColumnHandle>& p) const final {
    from_json_template<ColumnHandleType>(j, p);
  }

  void to_json(json& j, const std::shared_ptr<ConnectorInsertTableHandle>& p)
      const final {
    to_json_template<ConnectorInsertTableHandleType>(j, p);
  }
  void from_json(const json& j, std::shared_ptr<ConnectorInsertTableHandle>& p)
      const final {
    from_json_template<ConnectorInsertTableHandleType>(j, p);
  }
  void serialize(
      const std::shared_ptr<ConnectorInsertTableHandle>& proto,
      std::string& thrift) const final {
    serializeTemplate<ConnectorInsertTableHandleType>(proto, thrift);
  }
  void deserialize(
      const std::string& thrift,
      std::shared_ptr<ConnectorInsertTableHandle>& proto) const final {
    deserializeTemplate<ConnectorInsertTableHandleType>(thrift, proto);
  }

  void to_json(
      json& j,
      const std::shared_ptr<ConnectorDistributedProcedureHandle>& p)
      const final {
    to_json_template<ConnectorDistributedProcedureHandleType>(j, p);
  }
  void from_json(
      const json& j,
      std::shared_ptr<ConnectorDistributedProcedureHandle>& p) const final {
    from_json_template<ConnectorDistributedProcedureHandleType>(j, p);
  }

  void to_json(json& j, const std::shared_ptr<ConnectorOutputTableHandle>& p)
      const final {
    to_json_template<ConnectorOutputTableHandleType>(j, p);
  }
  void from_json(const json& j, std::shared_ptr<ConnectorOutputTableHandle>& p)
      const final {
    from_json_template<ConnectorOutputTableHandleType>(j, p);
  }
  void serialize(
      const std::shared_ptr<ConnectorOutputTableHandle>& proto,
      std::string& thrift) const final {
    serializeTemplate<ConnectorOutputTableHandleType>(proto, thrift);
  }
  void deserialize(
      const std::string& thrift,
      std::shared_ptr<ConnectorOutputTableHandle>& proto) const final {
    deserializeTemplate<ConnectorOutputTableHandleType>(thrift, proto);
  }

  void to_json(json& j, const std::shared_ptr<ConnectorSplit>& p) const final {
    to_json_template<ConnectorSplitType>(j, p);
  }
  void from_json(const json& j, std::shared_ptr<ConnectorSplit>& p)
      const final {
    from_json_template<ConnectorSplitType>(j, p);
  }
  void serialize(
      const std::shared_ptr<ConnectorSplit>& proto,
      std::string& thrift) const final {
    serializeTemplate<ConnectorSplitType>(proto, thrift);
  }
  void deserialize(
      const std::string& thrift,
      std::shared_ptr<ConnectorSplit>& proto) const final {
    deserializeTemplate<ConnectorSplitType>(thrift, proto);
  }

  void to_json(json& j, const std::shared_ptr<ConnectorPartitioningHandle>& p)
      const final {
    to_json_template<ConnectorPartitioningHandleType>(j, p);
  }
  void from_json(const json& j, std::shared_ptr<ConnectorPartitioningHandle>& p)
      const final {
    from_json_template<ConnectorPartitioningHandleType>(j, p);
  }

  void to_json(json& j, const std::shared_ptr<ConnectorTransactionHandle>& p)
      const final {
    to_json_template<ConnectorTransactionHandleType>(j, p);
  }
  void from_json(const json& j, std::shared_ptr<ConnectorTransactionHandle>& p)
      const final {
    from_json_template<ConnectorTransactionHandleType>(j, p);
  }
  void serialize(
      const std::shared_ptr<ConnectorTransactionHandle>& proto,
      std::string& thrift) const final {
    serializeTemplate<ConnectorTransactionHandleType>(proto, thrift);
  }
  void deserialize(
      const std::string& thrift,
      std::shared_ptr<ConnectorTransactionHandle>& proto) const final {
    deserializeTemplate<ConnectorTransactionHandleType>(thrift, proto);
  }

  void to_json(json& j, const std::shared_ptr<ConnectorDeleteTableHandle>& p)
      const final {
    to_json_template<ConnectorDeleteTableHandleType>(j, p);
  }
  void from_json(const json& j, std::shared_ptr<ConnectorDeleteTableHandle>& p)
      const final {
    from_json_template<ConnectorDeleteTableHandleType>(j, p);
  }
  void serialize(
      const std::shared_ptr<ConnectorDeleteTableHandle>& proto,
      std::string& thrift) const final {
    serializeTemplate<ConnectorDeleteTableHandleType>(proto, thrift);
  }
  void deserialize(
      const std::string& thrift,
      std::shared_ptr<ConnectorDeleteTableHandle>& proto) const final {
    deserializeTemplate<ConnectorDeleteTableHandleType>(thrift, proto);
  }

  void to_json(json& j, const std::shared_ptr<ConnectorIndexHandle>& p)
      const final {
    to_json_template<ConnectorIndexHandleType>(j, p);
  }
  void from_json(const json& j, std::shared_ptr<ConnectorIndexHandle>& p)
      const final {
    from_json_template<ConnectorIndexHandleType>(j, p);
  }

 private:
  template <typename DERIVED, typename BASE>
  static void to_json_template(
      json& j,
      const std::shared_ptr<BASE>& p,
      typename std::enable_if<std::is_base_of<BASE, DERIVED>::value, BASE>::
          type* = 0) {
    j = *std::static_pointer_cast<DERIVED>(p);
  }

  template <typename DERIVED, typename BASE>
  static void to_json_template(
      json&,
      const std::shared_ptr<BASE>&,
      typename std::enable_if<
          std::is_same<DERIVED, NotImplemented>::value,
          BASE>::type* = 0) {
    VELOX_NYI("Not implemented: {}", typeid(BASE).name());
  }

  template <typename DERIVED, typename BASE>
  static void from_json_template(
      const json& j,
      std::shared_ptr<BASE>& p,
      typename std::enable_if<std::is_base_of<BASE, DERIVED>::value, BASE>::
          type* = 0) {
    auto k = std::make_shared<DERIVED>();
    j.get_to(*k);
    p = k;
  }

  template <typename DERIVED, typename BASE>
  static void from_json_template(
      const json&,
      std::shared_ptr<BASE>&,
      typename std::enable_if<
          std::is_same<DERIVED, NotImplemented>::value,
          BASE>::type* = 0) {
    VELOX_NYI("Not implemented: {}", typeid(BASE).name());
  }

  template <typename DERIVED, typename BASE>
  static void serializeTemplate(
      const std::shared_ptr<BASE>&,
      std::string&,
      typename std::enable_if<
          std::is_same<DERIVED, NotImplemented>::value,
          BASE>::type* = 0) {
    VELOX_NYI("Not implemented: {}", typeid(BASE).name());
  }
  template <typename DERIVED, typename BASE>
  static void deserializeTemplate(
      const std::string&,
      std::shared_ptr<BASE>&,
      typename std::enable_if<
          std::is_same<DERIVED, NotImplemented>::value,
          BASE>::type* = 0) {
    VELOX_NYI("Not implemented: {}", typeid(BASE).name());
  }

  template <typename DERIVED, typename BASE>
  static void serializeTemplate(
      const std::shared_ptr<BASE>& proto,
      std::string& thrift,
      typename std::enable_if<std::is_base_of<BASE, DERIVED>::value, BASE>::
          type* = 0) {
    auto derived = *std::static_pointer_cast<DERIVED>(proto);
    thrift = derived.serialize(derived);
  }

  template <typename DERIVED, typename BASE>
  static void deserializeTemplate(
      const std::string& thrift,
      std::shared_ptr<BASE>& proto,
      typename std::enable_if<std::is_base_of<BASE, DERIVED>::value, BASE>::
          type* = 0) {
    std::shared_ptr<DERIVED> derived;
    proto = derived->deserialize(thrift, derived);
  }
};
using SystemConnectorProtocol = ConnectorProtocolTemplate<
    SystemTableHandle,
    SystemTableLayoutHandle,
    SystemColumnHandle,
    NotImplemented,
    NotImplemented,
    SystemSplit,
    SystemPartitioningHandle,
    SystemTransactionHandle,
    NotImplemented,
    NotImplemented,
    NotImplemented>;

using TvfNativeConnectorProtocol = ConnectorProtocolTemplate<
    SystemTableHandle,
    SystemTableLayoutHandle,
    SystemColumnHandle,
    NotImplemented,
    NotImplemented,
    NativeTableFunctionSplit,
    SystemPartitioningHandle,
    SystemTransactionHandle,
    NotImplemented,
    NotImplemented>;

} // namespace facebook::presto::protocol
