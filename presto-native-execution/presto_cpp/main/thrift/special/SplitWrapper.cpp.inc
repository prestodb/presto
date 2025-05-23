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

void toThrift(const facebook::presto::protocol::Split& split, SplitWrapper& thriftSplitWrapper) {
  toThrift(split, *thriftSplitWrapper.split_ref());
}
void toThrift(const facebook::presto::protocol::Split& split, std::string& thriftSplit) {
  json jsonSplit = split;
  std::string str = jsonSplit.dump();
  toThrift(str, thriftSplit);
}
void fromThrift(const SplitWrapper& thriftSplitWrapper, facebook::presto::protocol::Split& split) {
  fromThrift(*thriftSplitWrapper.split_ref(), split);
}
void fromThrift(const std::string& thriftSplit, facebook::presto::protocol::Split& split) {
  json j = json::parse(thriftSplit);
  split = j;
}
