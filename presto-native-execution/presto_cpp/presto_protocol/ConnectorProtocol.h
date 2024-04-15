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

#include "presto_cpp/presto_protocol/presto_protocol.h"
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

  virtual void to_json(
      json& j,
      const std::shared_ptr<ConnectorTableLayoutHandle>& p) const = 0;
  virtual void from_json(
      const json& j,
      std::shared_ptr<ConnectorTableLayoutHandle>& p) const = 0;

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

  virtual void to_json(
      json& j,
      const std::shared_ptr<ConnectorOutputTableHandle>& p) const = 0;
  virtual void from_json(
      const json& j,
      std::shared_ptr<ConnectorOutputTableHandle>& p) const = 0;

  virtual void to_json(json& j, const std::shared_ptr<ConnectorSplit>& p)
      const = 0;
  virtual void from_json(const json& j, std::shared_ptr<ConnectorSplit>& p)
      const = 0;

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

  virtual void to_json(
      json& j,
      const std::shared_ptr<ConnectorMetadataUpdateHandle>& p) const = 0;
  virtual void from_json(
      const json& j,
      std::shared_ptr<ConnectorMetadataUpdateHandle>& p) const = 0;
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
    typename ConnectorMetadataUpdateHandleType = NotImplemented>
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

  void to_json(json& j, const std::shared_ptr<ConnectorTableLayoutHandle>& p)
      const final {
    to_json_template<ConnectorTableLayoutHandleType>(j, p);
  }
  void from_json(const json& j, std::shared_ptr<ConnectorTableLayoutHandle>& p)
      const final {
    from_json_template<ConnectorTableLayoutHandleType>(j, p);
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

  void to_json(json& j, const std::shared_ptr<ConnectorOutputTableHandle>& p)
      const final {
    to_json_template<ConnectorOutputTableHandleType>(j, p);
  }
  void from_json(const json& j, std::shared_ptr<ConnectorOutputTableHandle>& p)
      const final {
    from_json_template<ConnectorOutputTableHandleType>(j, p);
  }

  void to_json(json& j, const std::shared_ptr<ConnectorSplit>& p) const final {
    to_json_template<ConnectorSplitType>(j, p);
  }
  void from_json(const json& j, std::shared_ptr<ConnectorSplit>& p)
      const final {
    from_json_template<ConnectorSplitType>(j, p);
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

  void to_json(json& j, const std::shared_ptr<ConnectorMetadataUpdateHandle>& p)
      const final {
    to_json_template<ConnectorMetadataUpdateHandleType>(j, p);
  }
  void from_json(
      const json& j,
      std::shared_ptr<ConnectorMetadataUpdateHandle>& p) const final {
    from_json_template<ConnectorMetadataUpdateHandleType>(j, p);
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
};

using HiveConnectorProtocol = ConnectorProtocolTemplate<
    HiveTableHandle,
    HiveTableLayoutHandle,
    HiveColumnHandle,
    HiveInsertTableHandle,
    HiveOutputTableHandle,
    HiveSplit,
    HivePartitioningHandle,
    HiveTransactionHandle,
    HiveMetadataUpdateHandle>;

using IcebergConnectorProtocol = ConnectorProtocolTemplate<
    IcebergTableHandle,
    IcebergTableLayoutHandle,
    IcebergColumnHandle,
    NotImplemented,
    NotImplemented,
    IcebergSplit,
    NotImplemented,
    HiveTransactionHandle,
    NotImplemented>;

using TpchConnectorProtocol = ConnectorProtocolTemplate<
    TpchTableHandle,
    TpchTableLayoutHandle,
    TpchColumnHandle,
    NotImplemented,
    NotImplemented,
    TpchSplit,
    TpchPartitioningHandle,
    TpchTransactionHandle,
    NotImplemented>;

} // namespace facebook::presto::protocol
