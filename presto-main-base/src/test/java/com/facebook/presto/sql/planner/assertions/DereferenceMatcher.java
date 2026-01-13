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
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.planner.LiteralInterpreter;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static java.util.Objects.requireNonNull;

/**
 * Matcher for DEREFERENCE expressions (field access from ROW types).
 * Matches expressions like: DEREFERENCE(base, index)
 * Supports matching specific index or a range of indices.
 */
public class DereferenceMatcher
        implements RvalueMatcher
{
    private final String baseAlias;
    private final int minIndex;
    private final int maxIndex;

    /**
     * Creates a matcher for a specific field index.
     */
    public DereferenceMatcher(String baseAlias, int fieldIndex)
    {
        this(baseAlias, fieldIndex, fieldIndex);
    }

    /**
     * Creates a matcher for a range of field indices [minIndex, maxIndex].
     */
    public DereferenceMatcher(String baseAlias, int minIndex, int maxIndex)
    {
        this.baseAlias = requireNonNull(baseAlias, "baseAlias is null");
        this.minIndex = minIndex;
        this.maxIndex = maxIndex;
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
        if (specialForm.getForm() != DEREFERENCE) {
            return false;
        }

        if (specialForm.getArguments().size() != 2) {
            return false;
        }

        // Check the base expression matches the expected alias
        RowExpression baseExpression = specialForm.getArguments().get(0);
        if (!(baseExpression instanceof VariableReferenceExpression)) {
            return false;
        }

        VariableReferenceExpression baseVariable = (VariableReferenceExpression) baseExpression;
        SymbolReference expectedBase = symbolAliases.get(baseAlias);
        if (!baseVariable.getName().equals(expectedBase.getName())) {
            return false;
        }

        // Check the index matches
        RowExpression indexExpression = specialForm.getArguments().get(1);
        if (!(indexExpression instanceof ConstantExpression)) {
            return false;
        }

        ConstantExpression indexConstant = (ConstantExpression) indexExpression;
        Object value = LiteralInterpreter.evaluate(TEST_SESSION.toConnectorSession(), indexConstant);
        if (!(value instanceof Long)) {
            return false;
        }

        long actualIndex = (Long) value;
        return actualIndex >= minIndex && actualIndex <= maxIndex;
    }

    @Override
    public String toString()
    {
        if (minIndex == maxIndex) {
            return String.format("DEREFERENCE(%s, %d)", baseAlias, minIndex);
        }
        return String.format("DEREFERENCE(%s, [%d-%d])", baseAlias, minIndex, maxIndex);
    }
}
