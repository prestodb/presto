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

// DeltaColumn is special since only a subset of the Java fields is used and
// the optional ones are carried as pointers.

namespace facebook::presto::protocol::delta {

struct DeltaColumn {
  std::shared_ptr<int64_t> id = {};
  std::shared_ptr<std::string> physicalName = {};
  std::string logicalName = {};
  std::string type = {};
  bool nullable = {};
  bool partition = {};
};

} // namespace facebook::presto::protocol::delta
