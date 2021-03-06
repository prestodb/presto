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
package com.facebook.presto.sql.planner.assertions;

import com.facebook.presto.Session;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.spi.plan.PlanNode;
import com.facebook.presto.spi.relation.VariableReferenceExpression;

import java.util.Optional;

import static com.facebook.presto.common.type.UnknownType.UNKNOWN;
import static java.util.Objects.requireNonNull;

/**
 * Just check alias is present; return self mapping from symbolAliasesMap.
 */
class AliasPresent
        implements RvalueMatcher
{
    private final String alias;

    AliasPresent(String alias)
    {
        this.alias = requireNonNull(alias, "alias can not be null");
    }

    @Override
    public Optional<VariableReferenceExpression> getAssignedVariable(PlanNode node, Session session, Metadata metadata, SymbolAliases symbolAliases)
    {
        return symbolAliases.getOptional(alias)
                .map(alias -> new VariableReferenceExpression(alias.getName(), UNKNOWN));
    }

    @Override
    public String toString()
    {
        return "has " + alias;
    }
}
