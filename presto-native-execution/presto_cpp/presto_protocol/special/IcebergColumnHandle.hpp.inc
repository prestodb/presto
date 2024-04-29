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

// IcebergColumnHandle is special since it needs an implementation of
// operator<().

namespace facebook::presto::protocol {

struct IcebergColumnHandle : public ColumnHandle {
  ColumnIdentity columnIdentity = {};
  Type type = {};
  std::shared_ptr<String> comment = {};
  ColumnType columnType = {};
  List<Subfield> requiredSubfields = {};

  IcebergColumnHandle() noexcept;

  bool operator<(const ColumnHandle& o) const override {
    return columnIdentity.name <
        dynamic_cast<const IcebergColumnHandle&>(o).columnIdentity.name;
  }
};

void to_json(json& j, const IcebergColumnHandle& p);
void from_json(const json& j, IcebergColumnHandle& p);

} // namespace facebook::presto::protocol