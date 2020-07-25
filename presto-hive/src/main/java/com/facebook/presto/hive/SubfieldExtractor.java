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
package com.facebook.presto.hive;

import com.facebook.presto.common.Subfield;
import com.facebook.presto.common.type.ArrayType;
import com.facebook.presto.common.type.MapType;
import com.facebook.presto.common.type.RowType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.function.FunctionHandle;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.ExpressionOptimizer;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.common.function.OperatorType.SUBSCRIPT;
import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static com.facebook.presto.hive.HiveSessionProperties.isRangeFiltersOnSubscriptsEnabled;
import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.google.common.base.Verify.verify;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public final class SubfieldExtractor
{
    private final StandardFunctionResolution functionResolution;
    private final ExpressionOptimizer expressionOptimizer;
    private final ConnectorSession connectorSession;

    public SubfieldExtractor(
            StandardFunctionResolution functionResolution,
            ExpressionOptimizer expressionOptimizer,
            ConnectorSession connectorSession)
    {
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
        this.expressionOptimizer = requireNonNull(expressionOptimizer, "expressionOptimzier is null");
        this.connectorSession = requireNonNull(connectorSession, "connectorSession is null");
    }

    public DomainTranslator.ColumnExtractor<Subfield> toColumnExtractor()
    {
        return (expression, domain) -> {
            Type type = expression.getType();

            // For complex types support only Filter IS_NULL and IS_NOT_NULL
            if (isComplexType(type) && !(domain.isOnlyNull() || (domain.getValues().isAll() && !domain.isNullAllowed()))) {
                return Optional.empty();
            }

            Optional<Subfield> subfield = extract(expression);
            // If the expression involves array or map subscripts, it is considered only if allowed by nested_columns_filter_enabled.
            if (hasSubscripts(subfield)) {
                if (isRangeFiltersOnSubscriptsEnabled(connectorSession)) {
                    return subfield;
                }
                return Optional.empty();
            }
            // The expression is a column or a set of nested struct field references.
            return subfield;
        };
    }

    private static boolean isComplexType(Type type)
    {
        return type instanceof ArrayType || type instanceof MapType || type instanceof RowType;
    }

    private static boolean hasSubscripts(Optional<Subfield> subfield)
    {
        return subfield.isPresent() && subfield.get().getPath().stream().anyMatch(Subfield.PathElement::isSubscript);
    }

    public Optional<Subfield> extract(RowExpression expression)
    {
        return toSubfield(expression, functionResolution, expressionOptimizer, connectorSession);
    }

    private static Optional<Subfield> toSubfield(
            RowExpression expression,
            StandardFunctionResolution functionResolution,
            ExpressionOptimizer expressionOptimizer,
            ConnectorSession connectorSession)
    {
        List<Subfield.PathElement> elements = new ArrayList<>();
        while (true) {
            if (expression instanceof VariableReferenceExpression) {
                Collections.reverse(elements);
                return Optional.of(new Subfield(((VariableReferenceExpression) expression).getName(), unmodifiableList(elements)));
            }

            if (expression instanceof SpecialFormExpression && ((SpecialFormExpression) expression).getForm() == DEREFERENCE) {
                SpecialFormExpression dereferenceExpression = (SpecialFormExpression) expression;
                RowExpression base = dereferenceExpression.getArguments().get(0);
                RowType baseType = (RowType) base.getType();

                RowExpression indexExpression = expressionOptimizer.optimize(
                        dereferenceExpression.getArguments().get(1),
                        ExpressionOptimizer.Level.OPTIMIZED,
                        connectorSession);

                if (indexExpression instanceof ConstantExpression) {
                    Object index = ((ConstantExpression) indexExpression).getValue();
                    if (index instanceof Number) {
                        Optional<String> fieldName = baseType.getFields().get(((Number) index).intValue()).getName();
                        if (fieldName.isPresent()) {
                            elements.add(new Subfield.NestedField(fieldName.get()));
                            expression = base;
                            continue;
                        }
                    }
                }
                return Optional.empty();
            }

            if (expression instanceof CallExpression && functionResolution.isSubscriptFunction(((CallExpression) expression).getFunctionHandle())) {
                List<RowExpression> arguments = ((CallExpression) expression).getArguments();
                RowExpression indexExpression = expressionOptimizer.optimize(
                        arguments.get(1),
                        ExpressionOptimizer.Level.OPTIMIZED,
                        connectorSession);

                if (indexExpression instanceof ConstantExpression) {
                    Object index = ((ConstantExpression) indexExpression).getValue();
                    if (index instanceof Number) {
                        elements.add(new Subfield.LongSubscript(((Number) index).longValue()));
                        expression = arguments.get(0);
                        continue;
                    }

                    if (isVarcharType(indexExpression.getType())) {
                        elements.add(new Subfield.StringSubscript(((Slice) index).toStringUtf8()));
                        expression = arguments.get(0);
                        continue;
                    }
                }
                return Optional.empty();
            }

            return Optional.empty();
        }
    }

    public RowExpression toRowExpression(Subfield subfield, Type columnType)
    {
        List<Subfield.PathElement> path = subfield.getPath();
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        types.add(columnType);
        Type type = columnType;
        for (int i = 0; i < path.size(); i++) {
            if (type instanceof RowType) {
                type = getFieldType((RowType) type, ((Subfield.NestedField) path.get(i)).getName());
                types.add(type);
            }
            else if (type instanceof ArrayType) {
                type = ((ArrayType) type).getElementType();
                types.add(type);
            }
            else if (type instanceof MapType) {
                type = ((MapType) type).getValueType();
                types.add(type);
            }
            else {
                verify(false, "Unexpected type: " + type);
            }
        }

        return toRowExpression(subfield, types.build());
    }

    private RowExpression toRowExpression(Subfield subfield, List<Type> types)
    {
        List<Subfield.PathElement> path = subfield.getPath();
        if (path.isEmpty()) {
            return new VariableReferenceExpression(subfield.getRootName(), types.get(0));
        }

        RowExpression base = toRowExpression(new Subfield(subfield.getRootName(), path.subList(0, path.size() - 1)), types.subList(0, types.size() - 1));
        Type baseType = types.get(types.size() - 2);
        Subfield.PathElement pathElement = path.get(path.size() - 1);
        if (pathElement instanceof Subfield.LongSubscript) {
            Type indexType = baseType instanceof MapType ? ((MapType) baseType).getKeyType() : BIGINT;
            FunctionHandle functionHandle = functionResolution.subscriptFunction(baseType, indexType);
            ConstantExpression index = new ConstantExpression(((Subfield.LongSubscript) pathElement).getIndex(), indexType);
            return new CallExpression(SUBSCRIPT.name(), functionHandle, types.get(types.size() - 1), ImmutableList.of(base, index));
        }

        if (pathElement instanceof Subfield.StringSubscript) {
            Type indexType = ((MapType) baseType).getKeyType();
            FunctionHandle functionHandle = functionResolution.subscriptFunction(baseType, indexType);
            ConstantExpression index = new ConstantExpression(Slices.utf8Slice(((Subfield.StringSubscript) pathElement).getIndex()), indexType);
            return new CallExpression(SUBSCRIPT.name(), functionHandle, types.get(types.size() - 1), ImmutableList.of(base, index));
        }

        if (pathElement instanceof Subfield.NestedField) {
            Subfield.NestedField nestedField = (Subfield.NestedField) pathElement;
            return new SpecialFormExpression(DEREFERENCE, types.get(types.size() - 1), base, new ConstantExpression(getFieldIndex((RowType) baseType, nestedField.getName()), INTEGER));
        }

        verify(false, "Unexpected path element: " + pathElement);
        return null;
    }

    private static Type getFieldType(RowType rowType, String fieldName)
    {
        for (RowType.Field field : rowType.getFields()) {
            verify(field.getName().isPresent());
            if (field.getName().get().equals(fieldName)) {
                return field.getType();
            }
        }
        verify(false, "Unexpected field name: " + fieldName);
        return null;
    }

    private static long getFieldIndex(RowType rowType, String fieldName)
    {
        List<RowType.Field> fields = rowType.getFields();
        for (int i = 0; i < fields.size(); i++) {
            RowType.Field field = fields.get(i);
            verify(field.getName().isPresent());
            if (field.getName().get().equals(fieldName)) {
                return i;
            }
        }
        verify(false, "Unexpected field name: " + fieldName);
        return -1;
    }
}
