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

// TpchTransactionHandle is special since
// the corresponding class in Java is an enum.

namespace facebook::presto::protocol {

void to_json(json& j, const TpchTransactionHandle& p) {
  j = json::array();
  j.push_back(p._type);
  j.push_back(p.instance);
}

void from_json(const json& j, TpchTransactionHandle& p) {
  j[0].get_to(p._type);
  j[1].get_to(p.instance);
}
} // namespace facebook::presto::protocol
