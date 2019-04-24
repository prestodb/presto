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

import com.facebook.presto.spi.Subfield;
import com.facebook.presto.spi.function.StandardFunctionResolution;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.DomainTranslator;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.RowType;

import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.relation.SpecialFormExpression.Form.DEREFERENCE;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.util.Collections.unmodifiableList;
import static java.util.Objects.requireNonNull;

public final class SubfieldExtractor
        implements DomainTranslator.ColumnExtractor<Subfield>
{
    private final StandardFunctionResolution functionResolution;

    public SubfieldExtractor(StandardFunctionResolution functionResolution)
    {
        this.functionResolution = requireNonNull(functionResolution, "functionResolution is null");
    }

    @Override
    public Optional<Subfield> extract(RowExpression expression)
    {
        return toSubfield(expression, functionResolution);
    }

    private static Optional<Subfield> toSubfield(RowExpression expression, StandardFunctionResolution functionResolution)
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

                RowExpression indexExpression = dereferenceExpression.getArguments().get(1);
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
                RowExpression indexExpression = arguments.get(1);
                if (indexExpression instanceof ConstantExpression) {
                    Object index = ((ConstantExpression) indexExpression).getValue();
                    if (index instanceof Number) {
                        elements.add(new Subfield.LongSubscript(((Number) index).longValue()));
                        expression = arguments.get(0);
                        continue;
                    }

                    if (isVarcharType(indexExpression.getType())) {
                        elements.add(new Subfield.StringSubscript(String.valueOf(index)));
                        expression = arguments.get(0);
                        continue;
                    }
                }
                return Optional.empty();
            }

            return Optional.empty();
        }
    }
}
