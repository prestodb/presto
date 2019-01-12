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
package io.prestosql.sql.planner.sanity;

import io.prestosql.Session;
import io.prestosql.execution.warnings.WarningCollector;
import io.prestosql.metadata.Metadata;
import io.prestosql.sql.analyzer.ExpressionTreeUtils;
import io.prestosql.sql.parser.SqlParser;
import io.prestosql.sql.planner.ExpressionExtractor;
import io.prestosql.sql.planner.TypeProvider;
import io.prestosql.sql.planner.plan.PlanNode;
import io.prestosql.sql.tree.Identifier;

import java.util.List;

public final class NoIdentifierLeftChecker
        implements PlanSanityChecker.Checker
{
    @Override
    public void validate(PlanNode plan, Session session, Metadata metadata, SqlParser sqlParser, TypeProvider types, WarningCollector warningCollector)
    {
        List<Identifier> identifiers = ExpressionTreeUtils.extractExpressions(ExpressionExtractor.extractExpressions(plan), Identifier.class);
        if (!identifiers.isEmpty()) {
            throw new IllegalStateException("Unexpected identifier in logical plan: " + identifiers.get(0));
        }
    }
}
