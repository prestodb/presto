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
package com.facebook.presto.sql.analyzer;

import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.Node;
import com.facebook.presto.sql.util.AstUtils;

import java.util.List;
import java.util.Map;
import java.util.Objects;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

/**
 * Extract expressions that are references to a given scope.
 */
class ScopeReferenceExtractor
{
    private ScopeReferenceExtractor() {}

    public static boolean hasReferencesToScope(Node node, Analysis analysis, Scope scope)
    {
        return !getReferencesToScope(node, analysis, scope).isEmpty();
    }

    public static List<Expression> getReferencesToScope(Node node, Analysis analysis, Scope scope)
    {
        Map<Expression, FieldId> columnReferences = analysis.getColumnReferenceFields();

        List<Expression> referencesToScope = AstUtils.preOrder(node)
                .filter(columnReferences::containsKey)
                .map(Expression.class::cast)
                .filter(expression -> isReferenceToScope(expression, scope, columnReferences))
                .collect(toImmutableList());

        return referencesToScope;
    }

    private static boolean isReferenceToScope(Expression node, Scope scope, Map<Expression, FieldId> columnReferences)
    {
        FieldId fieldId = requireNonNull(columnReferences.get(node), () -> "No FieldId for " + node);
        return isFieldFromScope(fieldId, scope);
    }

    public static boolean isFieldFromScope(FieldId fieldId, Scope scope)
    {
        return Objects.equals(fieldId.getRelationId(), scope.getRelationId());
    }
}
