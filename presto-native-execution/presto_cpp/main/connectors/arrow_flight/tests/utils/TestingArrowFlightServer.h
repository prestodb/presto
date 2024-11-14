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

#include <arrow/api.h>
#include <arrow/flight/api.h>

namespace facebook::presto::test {

/// Test Flight server which supports DoGet operations.
/// Maintains a list of named arrow tables,
///
/// Normally, the tickets would be obtained by calling GetFlightInfo,
/// but since this is done by the coordinator this part is omitted.
/// Instead, the ticket is simply the name of the table to fetch.
class TestingArrowFlightServer : public arrow::flight::FlightServerBase {
 public:
  TestingArrowFlightServer() = default;

  void updateTable(
      const std::string& name,
      const std::shared_ptr<arrow::Table>& table) {
    tables_.emplace(name, table);
  }

  void removeTable(const std::string& name) {
    tables_.erase(name);
  }

  void setBatchSize(int64_t batchSize) {
    batchSize_ = std::make_optional<int64_t>(batchSize);
  }

  arrow::Status DoGet(
      const arrow::flight::ServerCallContext& context,
      const arrow::flight::Ticket& request,
      std::unique_ptr<arrow::flight::FlightDataStream>* stream) override;

 private:
  std::unordered_map<std::string, std::shared_ptr<arrow::Table>> tables_;
  std::optional<int64_t> batchSize_;
};

} // namespace facebook::presto::test
