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

void toThrift(const facebook::presto::protocol::QualifiedObjectName& proto, QualifiedObjectName& thrift) {
  std::vector<std::string> parts;
  folly::split(".", proto, parts);
  if (parts.size() == 3) {
    thrift.catalogName_ref() = parts[0];
    thrift.schemaName_ref()= parts[1];
    thrift.objectName_ref() = parts[2];
  }
}
void fromThrift(const QualifiedObjectName& thrift, facebook::presto::protocol::QualifiedObjectName& proto) {
  std::string qualifiedObjectNameStr =
    *thrift.catalogName_ref() + "." +
    *thrift.schemaName_ref() + "." +
    *thrift.objectName_ref();
  fromThrift(qualifiedObjectNameStr, proto);
}
