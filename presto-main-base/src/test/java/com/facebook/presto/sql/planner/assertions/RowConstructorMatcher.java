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
import com.facebook.presto.spi.plan.ProjectNode;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.ROW_CONSTRUCTOR;
import static java.util.Objects.requireNonNull;

/**
 * Matcher for ROW_CONSTRUCTOR expressions.
 * Matches expressions like: ROW_CONSTRUCTOR(field1, field2, ...)
 * Order-agnostic: checks that all expected fields are present in any order.
 */
public class RowConstructorMatcher
        implements RvalueMatcher
{
    private final Set<String> fieldAliases;

    public RowConstructorMatcher(List<String> fieldAliases)
    {
        this.fieldAliases = ImmutableSet.copyOf(requireNonNull(fieldAliases, "fieldAliases is null"));
    }

    public RowConstructorMatcher(String... fieldAliases)
    {
        this(ImmutableList.copyOf(fieldAliases));
    }

    @Override
    public Optional<VariableReferenceExpression> getAssignedVariable(
            PlanNode node,
            Session session,
            Metadata metadata,
            SymbolAliases symbolAliases)
    {
        if (!(node instanceof ProjectNode)) {
            return Optional.empty();
        }

        ProjectNode projectNode = (ProjectNode) node;
        Map<VariableReferenceExpression, RowExpression> assignments = projectNode.getAssignments().getMap();

        for (Map.Entry<VariableReferenceExpression, RowExpression> entry : assignments.entrySet()) {
            RowExpression expression = entry.getValue();

            if (matches(expression, symbolAliases)) {
                return Optional.of(entry.getKey());
            }
        }

        return Optional.empty();
    }

    private boolean matches(RowExpression expression, SymbolAliases symbolAliases)
    {
        if (!(expression instanceof SpecialFormExpression)) {
            return false;
        }

        SpecialFormExpression specialForm = (SpecialFormExpression) expression;
        if (specialForm.getForm() != ROW_CONSTRUCTOR) {
            return false;
        }

        List<RowExpression> arguments = specialForm.getArguments();
        if (arguments.size() != fieldAliases.size()) {
            return false;
        }

        // Collect actual field names from the ROW_CONSTRUCTOR arguments
        Set<String> actualFieldNames = new HashSet<>();
        for (RowExpression fieldExpression : arguments) {
            if (!(fieldExpression instanceof VariableReferenceExpression)) {
                return false;
            }
            actualFieldNames.add(((VariableReferenceExpression) fieldExpression).getName());
        }

        // Build set of expected field names from aliases
        Set<String> expectedFieldNames = new HashSet<>();
        for (String alias : fieldAliases) {
            SymbolReference expectedField = symbolAliases.get(alias);
            expectedFieldNames.add(expectedField.getName());
        }

        // Check that all expected fields are present (order-agnostic)
        return actualFieldNames.equals(expectedFieldNames);
    }

    @Override
    public String toString()
    {
        return String.format("ROW_CONSTRUCTOR(%s)", String.join(", ", fieldAliases));
    }
}
