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

void toThrift(const facebook::presto::protocol::SelectedRoleType& proto, Type& thrift) {
  thrift = (Type)(static_cast<int>(proto));
}
void fromThrift(const Type& thrift, facebook::presto::protocol::SelectedRoleType& proto) {
  proto = (facebook::presto::protocol::SelectedRoleType)(static_cast<int>(thrift));
}
