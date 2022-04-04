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
namespace facebook::presto::protocol {
struct MergeJoinNode {
  PlanNodeId id = {};
  // JoinNodeType is referenced as JoinNode.Type in Presto
  // Since presto_cpp codegen can't nicely handle inner class references
  // So a special hard-coded template is required here
  JoinNodeType type = {};
  std::shared_ptr<PlanNode> left = {};
  std::shared_ptr<PlanNode> right = {};
  // EquiJoinClause is referenced as JoinNode.EquiJoinClause in Presto
  List<EquiJoinClause> criteria = {};
  List<VariableReferenceExpression> outputVariables = {};
  std::shared_ptr<std::shared_ptr<RowExpression>> filter = {};
  std::shared_ptr<VariableReferenceExpression> leftHashVariable = {};
  std::shared_ptr<VariableReferenceExpression> rightHashVariable = {};
};
void to_json(json& j, const MergeJoinNode& p);
void from_json(const json& j, MergeJoinNode& p);
} // namespace facebook::presto::protocol