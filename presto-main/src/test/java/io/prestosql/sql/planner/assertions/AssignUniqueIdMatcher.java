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
import io.prestosql.sql.planner.plan.AssignUniqueId;
import io.prestosql.sql.planner.plan.PlanNode;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;

public class AssignUniqueIdMatcher
        implements RvalueMatcher
{
    @Override
    public Optional<Symbol> getAssignedSymbol(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        if (!(node instanceof AssignUniqueId)) {
            return Optional.empty();
        }

        AssignUniqueId assignUniqueIdNode = (AssignUniqueId) node;

        return Optional.of(assignUniqueIdNode.getIdColumn());
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .toString();
    }
}
