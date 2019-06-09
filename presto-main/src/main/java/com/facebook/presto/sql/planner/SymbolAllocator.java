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

import com.facebook.presto.spi.VariableAllocator;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.sql.analyzer.Field;
import com.facebook.presto.sql.tree.Expression;
import com.facebook.presto.sql.tree.FunctionCall;
import com.facebook.presto.sql.tree.GroupingOperation;
import com.facebook.presto.sql.tree.Identifier;
import com.facebook.presto.sql.tree.QualifiedName;
import com.facebook.presto.sql.tree.SymbolReference;
import com.google.common.primitives.Ints;

import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;

public class SymbolAllocator
        implements VariableAllocator
{
    private final Map<Symbol, Type> symbols;
    private int nextId;

    public SymbolAllocator()
    {
        symbols = new HashMap<>();
    }

    public SymbolAllocator(Map<Symbol, Type> initial)
    {
        symbols = new HashMap<>(initial);
    }

    public VariableReferenceExpression newVariable(Symbol symbolHint)
    {
        checkArgument(symbols.containsKey(symbolHint), "symbolHint not in symbols map");
        return newVariable(symbolHint.getName(), symbols.get(symbolHint));
    }

    public VariableReferenceExpression newVariable(VariableReferenceExpression variableHint)
    {
        return newVariable(variableHint.getName(), variableHint.getType());
    }

    public VariableReferenceExpression newVariable(QualifiedName nameHint, Type type)
    {
        return newVariable(nameHint.getSuffix(), type, null);
    }

    public Symbol newSymbol(String nameHint, Type type)
    {
        return newSymbol(nameHint, type, null);
    }

    @Override
    public VariableReferenceExpression newVariable(String nameHint, Type type)
    {
        return newVariable(nameHint, type, null);
    }

    public VariableReferenceExpression newHashVariable()
    {
        return newVariable("$hashValue", BigintType.BIGINT);
    }

    @Override
    public VariableReferenceExpression newVariable(String nameHint, Type type, String suffix)
    {
        Symbol symbol = newSymbol(nameHint, type, suffix);
        return new VariableReferenceExpression(symbol.getName(), type);
    }

    public Symbol newSymbol(String nameHint, Type type, String suffix)
    {
        requireNonNull(nameHint, "name is null");
        requireNonNull(type, "type is null");

        // TODO: workaround for the fact that QualifiedName lowercases parts
        nameHint = nameHint.toLowerCase(ENGLISH);

        // don't strip the tail if the only _ is the first character
        int index = nameHint.lastIndexOf("_");
        if (index > 0) {
            String tail = nameHint.substring(index + 1);

            // only strip if tail is numeric or _ is the last character
            if (Ints.tryParse(tail) != null || index == nameHint.length() - 1) {
                nameHint = nameHint.substring(0, index);
            }
        }

        String unique = nameHint;

        if (suffix != null) {
            unique = unique + "$" + suffix;
        }

        String attempt = unique;
        while (symbols.containsKey(new Symbol(attempt))) {
            attempt = unique + "_" + nextId();
        }

        Symbol symbol = new Symbol(attempt);
        symbols.put(symbol, type);
        return symbol;
    }

    public VariableReferenceExpression newVariable(Expression expression, Type type)
    {
        Symbol symbol = newSymbol(expression, type);
        return new VariableReferenceExpression(symbol.getName(), type);
    }

    public Symbol newSymbol(Expression expression, Type type)
    {
        return newSymbol(expression, type, null);
    }

    public Symbol newSymbol(Expression expression, Type type, String suffix)
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

        return newSymbol(nameHint, type, suffix);
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
        return newVariable(nameHint, type, suffix);
    }

    public Symbol newSymbol(Field field)
    {
        String nameHint = field.getName().orElse("field");
        return newSymbol(nameHint, field.getType());
    }

    public VariableReferenceExpression newVariable(Field field)
    {
        return newVariable(field.getName().orElse("field"), field.getType(), null);
    }

    public TypeProvider getTypes()
    {
        return TypeProvider.viewOf(symbols);
    }

    private int nextId()
    {
        return nextId++;
    }

    public VariableReferenceExpression toVariableReference(Symbol symbol)
    {
        return new VariableReferenceExpression(symbol.getName(), getTypes().get(symbol));
    }

    public List<VariableReferenceExpression> toVariableReferences(Collection<Symbol> symbols)
    {
        return symbols.stream()
                .map(symbol -> new VariableReferenceExpression(symbol.getName(), getTypes().get(symbol)))
                .collect(toImmutableList());
    }
}
