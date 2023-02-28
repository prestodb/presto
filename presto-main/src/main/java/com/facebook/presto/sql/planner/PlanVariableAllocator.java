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
package com.facebook.presto.sql.planner;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.SourceLocation;
import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.SymbolReference;

import java.util.Collection;
import java.util.Optional;

import static com.facebook.presto.sql.analyzer.ExpressionTreeUtils.getSourceLocation;

// TODO: this class should eventually be removed, consumers should use VariableAllocator
public class PlanVariableAllocator
        extends VariableAllocator
{
    public PlanVariableAllocator()
    {
    }

    public PlanVariableAllocator(Collection<VariableReferenceExpression> initial)
    {
        super(initial);
    }

    public VariableReferenceExpression newVariable(Expression expression, Type type)
    {
        return newVariable(expression, type, null);
    }

    public VariableReferenceExpression newVariable(Expression expression, Type type, String suffix)
    {
        String nameHint = "expr";
        if (expression instanceof Identifier) {
            nameHint = ((Identifier) expression).getValue();
        }
        else if (expression instanceof FunctionCall) {
            nameHint = ((FunctionCall) expression).getName().getSuffix();
        }
        else if (expression instanceof SymbolReference) {
            nameHint = ((SymbolReference) expression).getName();
        }
        else if (expression instanceof GroupingOperation) {
            nameHint = "grouping";
        }
        return newVariable(getSourceLocation(expression), nameHint, type, suffix);
    }

    public VariableReferenceExpression newVariable(Field field)
    {
        return newVariable(getSourceLocation(field.getNodeLocation()), field);
    }

    public VariableReferenceExpression newVariable(Optional<SourceLocation> sourceLocation, Field field)
    {
        return newVariable(sourceLocation, field.getName().orElse("field"), field.getType(), null);
    }

    public TypeProvider getTypes()
    {
        return TypeProvider.viewOf(variables);
    }

    public VariableReferenceExpression toVariableReference(Expression expression)
    {
        checkArgument(expression instanceof SymbolReference, "Unexpected expression: " + expression);
        String name = ((SymbolReference) expression).getName();
        checkArgument(variables.containsKey(name), "variable map does not contain name " + name);
        return new VariableReferenceExpression(getSourceLocation(expression), name, variables.get(name));
    }
}
