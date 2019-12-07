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
package com.facebook.presto.operator.project;

import com.facebook.presto.operator.CompletedWork;
import com.facebook.presto.operator.DriverYieldSignal;
import com.facebook.presto.operator.Work;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RowBlock;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Optional;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class DereferencePageProjection
        implements PageProjection
{
    private final Type inputType;
    private final Type outputType;
    private final InputChannels inputChannels;
    private final List<Integer> fields;

    public DereferencePageProjection(int inputChannel, Type inputType, Type outputType, List<Integer> fields)
    {
        this.inputType = requireNonNull(inputType, "inputType is null");
        this.outputType = requireNonNull(inputType, "outputType is null");
        this.inputChannels = new InputChannels(inputChannel);
        this.fields = requireNonNull(fields, "fields is null");
    }

    @Override
    public Type getType()
    {
        return outputType;
    }

    @Override
    public boolean isDeterministic()
    {
        return true;
    }

    @Override
    public InputChannels getInputChannels()
    {
        return inputChannels;
    }

    @Override
    public Work<Block> project(ConnectorSession session, DriverYieldSignal yieldSignal, Page page, SelectedPositions selectedPositions)
    {
        Block block = requireNonNull(page, "page is null").getBlock(0);
        requireNonNull(selectedPositions, "selectedPositions is null");

        Block nestedBlock = block;
        Type nestedType = inputType;
        for (int field : fields) {
            nestedType = nestedType.getTypeParameters().get(field);
            nestedBlock = ((RowBlock) nestedBlock).getFieldBlock(field, nestedType);
        }
        return new CompletedWork<>(nestedBlock);
    }

    public static Optional<DereferencePageProjection> fromRowExpression(RowExpression projection)
    {
        if (!(projection instanceof SpecialFormExpression)) {
            return Optional.empty();
        }

        return extractDereferencesRecursively(projection, projection.getType(), ImmutableList.builder());
    }

    private static Optional<DereferencePageProjection> extractDereferencesRecursively(RowExpression projection, Type type, ImmutableList.Builder<Integer> fields)
    {
        if (projection instanceof InputReferenceExpression) {
            InputReferenceExpression input = (InputReferenceExpression) projection;
            return Optional.of(new DereferencePageProjection(input.getField(), input.getType(), type, fields.build().reverse()));
        }

        if (!(projection instanceof SpecialFormExpression)) {
            return Optional.empty();
        }

        SpecialFormExpression specialForm = (SpecialFormExpression) projection;
        if (specialForm.getForm() != SpecialFormExpression.Form.DEREFERENCE) {
            return Optional.empty();
        }

        List<RowExpression> arguments = specialForm.getArguments();
        RowExpression field = arguments.get(1);

        if (!(field instanceof ConstantExpression)) {
            return Optional.empty();
        }

        fields.add(toIntExact((long) ((ConstantExpression) field).getValue()));
        return extractDereferencesRecursively(arguments.get(0), type, fields);
    }
}
