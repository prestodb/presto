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

// DeltaColumnHandle is special since it needs an implementation of
// operator<().

namespace facebook::presto::protocol::delta {

struct DeltaColumnHandle : public ColumnHandle {
  String name = {};
  TypeSignature dataType = {};
  ColumnType columnType = {};
  std::shared_ptr<Subfield> subfield = {};

  DeltaColumnHandle() noexcept;

  bool operator<(const ColumnHandle& o) const override {
    return name < dynamic_cast<const DeltaColumnHandle&>(o).name;
  }
};

void to_json(json& j, const DeltaColumnHandle& p);
void from_json(const json& j, DeltaColumnHandle& p);

} // namespace facebook::presto::protocol::delta
