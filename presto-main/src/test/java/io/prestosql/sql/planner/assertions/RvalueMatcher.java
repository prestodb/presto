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
package io.prestosql.sql.planner.assertions;

import io.prestosql.Session;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.planner.Symbol;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.Optional;

public interface RvalueMatcher
{
    /**
     * Get the unique symbol that is assigned an rvalue matched by the RvalueMatcher in node.
     * RvalueMatchers can match anything that can be assigned to a Symbol, and may be specialized
     * based on the type of the node or the type of the value that is being assigned. For example,
     * TableScanNodes assign ColumnHandles to Symbols and AggregationNodes assign FunctionCalls
     * to Symbols.
     * <p>
     * The assigned symbol is identified by matching the value on the right side of the assignment;
     * the rvalue. If no match is found in the node, getAssignedSymbol must return Optional.empty().
     */
    Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases);
}
