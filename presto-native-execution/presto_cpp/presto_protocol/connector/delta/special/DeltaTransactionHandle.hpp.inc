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

// DeltaTransactionHandle is special since
// the corresponding class in Java is an enum.

namespace facebook::presto::protocol::delta {

struct DeltaTransactionHandle : public ConnectorTransactionHandle {
  String instance = {};
};

void to_json(json& j, const DeltaTransactionHandle& p);

void from_json(const json& j, DeltaTransactionHandle& p);

} // namespace facebook::presto::protocol::delta
