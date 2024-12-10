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
#include "presto_cpp/main/connectors/arrow_flight/tests/utils/TestFlightServer.h"

namespace facebook::presto::connector::arrow_flight::test {

using namespace arrow::flight;

arrow::Status TestFlightServer::DoGet(
    const ServerCallContext& context,
    const Ticket& request,
    std::unique_ptr<FlightDataStream>* stream) {
  auto it = tables_.find(request.ticket);
  if (it == tables_.end()) {
    return arrow::Status::KeyError("requested table does not exist");
  }
  auto& table = it->second;
  auto reader = std::make_shared<arrow::TableBatchReader>(table);
  *stream = std::make_unique<RecordBatchStream>(std::move(reader));
  return arrow::Status::OK();
}

} // namespace facebook::presto::connector::arrow_flight::test
