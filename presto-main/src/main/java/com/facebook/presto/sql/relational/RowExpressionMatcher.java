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
package com.facebook.presto.sql.relational;

import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RowBlock;
import com.facebook.presto.spi.relation.CallExpression;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.facebook.presto.spi.relation.InputReferenceExpression;
import com.facebook.presto.spi.relation.LambdaDefinitionExpression;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.spi.relation.RowExpressionVisitor;
import com.facebook.presto.spi.relation.SpecialFormExpression;
import com.facebook.presto.spi.relation.VariableReferenceExpression;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.DynamicSliceOutput;
import io.airlift.slice.Slice;
import io.airlift.slice.SliceOutput;

import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import static java.lang.Math.toIntExact;
import static java.util.Objects.requireNonNull;

public class RowExpressionMatcher
{
    public boolean match(RowExpression actual, RowExpression expected)
    {
        return actual.accept(new RowExpressionMatchVisitor(), expected);
    }

    public boolean match(RowExpression actual, RowExpression expected, Map<String, String> aliases)
    {
        return actual.accept(new RowExpressionMatchVisitor(aliases), expected);
    }

    public class RowExpressionMatchVisitor
            implements RowExpressionVisitor<Boolean, RowExpression>
    {
        private Map<String, String> aliases;

        public RowExpressionMatchVisitor()
        {
            this.aliases = new HashMap<>();
        }

        public RowExpressionMatchVisitor(Map<String, String> aliases)
        {
            requireNonNull(aliases);
            this.aliases = ImmutableMap.copyOf(aliases);
        }

        public Map<String, String> getAliases()
        {
            return aliases;
        }

        @Override
        public Boolean visitCall(CallExpression actualCall, RowExpression expectedPattern)
        {
            if (!(expectedPattern instanceof CallExpression)) {
                return false;
            }

            if (!actualCall.getType().equals(expectedPattern.getType())) {
                return false;
            }

            if (!actualCall.getFunctionHandle().equals(((CallExpression) expectedPattern).getFunctionHandle())) {
                return false;
            }

            return compare(actualCall.getArguments(), ((CallExpression) expectedPattern).getArguments());
        }

        @Override
        public Boolean visitInputReference(InputReferenceExpression reference, RowExpression expected)
        {
            if (!(expected instanceof InputReferenceExpression)) {
                return false;
            }

            if (reference.getField() != ((InputReferenceExpression) expected).getField()) {
                return false;
            }

            if (!reference.getType().equals(expected.getType())) {
                return false;
            }

            return true;
        }

        @Override
        public Boolean visitConstant(ConstantExpression actual, RowExpression expected)
        {
            if (!(expected instanceof ConstantExpression)) {
                return false;
            }

            return compareLiteral(actual, (ConstantExpression) expected, aliases);
        }

        @Override
        public Boolean visitLambda(LambdaDefinitionExpression actual, RowExpression expected)
        {
            if (!(expected instanceof LambdaDefinitionExpression)) {
                return false;
            }

            return compareLamdaDefinitionExpression(actual, (LambdaDefinitionExpression) expected);
        }

        @Override
        public Boolean visitVariableReference(VariableReferenceExpression reference, RowExpression expected)
        {
            if (!(expected instanceof VariableReferenceExpression)) {
                return false;
            }

            return compareVariableReferenceExpression(reference, (VariableReferenceExpression) expected, aliases);
        }

        @Override
        public Boolean visitSpecialForm(SpecialFormExpression specialForm, RowExpression expected)
        {
            if (!(expected instanceof SpecialFormExpression)) {
                return false;
            }
            if (!specialForm.getType().equals(expected.getType())) {
                return false;
            }

            if (specialForm.getForm() != ((SpecialFormExpression) expected).getForm()) {
                return false;
            }

            return compare(specialForm.getArguments(), ((SpecialFormExpression) expected).getArguments());
        }

        private boolean compare(List<RowExpression> actual, List<RowExpression> expected)
        {
            if (actual.size() != expected.size()) {
                return false;
            }

            Iterator<RowExpression> actualArgsItr = actual.iterator();
            Iterator<RowExpression> expectedArgsItr = expected.iterator();

            while (actualArgsItr.hasNext() && expectedArgsItr.hasNext()) {
                RowExpression actualArgument = actualArgsItr.next();
                RowExpression expectedArgument = expectedArgsItr.next();

                boolean isMatch = match(actualArgument, expectedArgument, aliases);
                if (!isMatch) {
                    return false;
                }
            }

            return true;
        }
    }

    protected boolean compareVariableReferenceExpression(VariableReferenceExpression actual, VariableReferenceExpression expected, Map<String, String> aliases)
    {
        if (!actual.getType().equals(expected.getType())) {
            return false;
        }

        if (!(actual.getName().compareTo(expected.getName()) == 0 || actual.getName().compareTo(aliases.getOrDefault(expected.getName(), "")) == 0 || expected.getName().compareTo(aliases.getOrDefault(actual.getName(), "")) == 0)) {
            return false;
        }

        return true;
    }

    protected boolean compareLiteral(ConstantExpression actual, ConstantExpression expected, Map<String, String> aliases)
    {
        Type type = actual.getType();
        if (type instanceof RowType) {
            RowType actualRowType = (RowType) actual.getType();
            RowType expectedRowType = (RowType) expected.getType();

            List<RowType.Field> actualFields = actualRowType.getFields();
            List<RowType.Field> expectedFields = expectedRowType.getFields();

            if (actualFields.size() != expectedFields.size()) {
                return false;
            }

            Iterator<RowType.Field> actualFieldItr = actualFields.iterator();
            Iterator<RowType.Field> expectedFieldItr = expectedFields.iterator();

            while (actualFieldItr.hasNext() && expectedFieldItr.hasNext()) {
                RowType.Field actualField = actualFieldItr.next();
                RowType.Field expectedField = expectedFieldItr.next();

                if (!actualField.getType().equals(expectedField.getType())) {
                    return false;
                }

                String actualFieldName = actualField.getName().orElse("");
                String expectedFieldName = expectedField.getName().orElse("");

                if (!(actualFieldName.compareTo(expectedFieldName) == 0 || actualFieldName.compareTo(aliases.getOrDefault(expectedFieldName, "")) == 0 || expectedFieldName.compareTo(aliases.getOrDefault(actualFieldName, "")) == 0)) {
                    return false;
                }
            }

            Object actualObj = actual.getValue();
            Object expectedObj = expected.getValue();
            if (!(actualObj instanceof RowBlock) || !(expectedObj instanceof RowBlock)) {
                return false;
            }

            SliceOutput actualOutput = new DynamicSliceOutput(toIntExact(((Block) actualObj).getSizeInBytes()));
            SliceOutput expectedOutput = new DynamicSliceOutput(toIntExact(((Block) expectedObj).getSizeInBytes()));

            Slice actualSlice = actualOutput.slice();
            Slice expectedSlice = expectedOutput.slice();
            if (!actualSlice.equals(expectedSlice)) {
                return false;
            }

            return true;
        }

        if (type instanceof ArrayType) {
            Type actualElementType = ((ArrayType) actual.getType()).getElementType();
            Type expectedElementType = ((ArrayType) expected.getType()).getElementType();

            if (actualElementType != expectedElementType) {
                return false;
            }

            SliceOutput actualOutput = new DynamicSliceOutput(toIntExact(((Block) actual.getValue()).getSizeInBytes()));
            SliceOutput expectedOutput = new DynamicSliceOutput(toIntExact(((Block) expected.getValue()).getSizeInBytes()));

            Slice actualSlice = actualOutput.slice();
            Slice expectedSlice = expectedOutput.slice();
            if (!actualSlice.equals(expectedSlice)) {
                return false;
            }

            return true;
        }

        return actual.equals(expected);
    }

    private boolean compareLamdaDefinitionExpression(LambdaDefinitionExpression actual, LambdaDefinitionExpression expected)
    {
        List<Type> actualArgTypes = actual.getArgumentTypes();
        List<Type> expectedArgTypes = expected.getArgumentTypes();
        if (!actualArgTypes.equals(expectedArgTypes)) {
            return false;
        }

        Iterator<String> actualArgItr = actual.getArguments().iterator();
        Iterator<String> expectedArgItr = expected.getArguments().iterator();

        ImmutableMap.Builder<String, String> builder = ImmutableMap.builder();
        while (actualArgItr.hasNext() && expectedArgItr.hasNext()) {
            String actualArg = actualArgItr.next();
            String expectedArg = expectedArgItr.next();

            builder.put(actualArg, expectedArg);
            builder.put(expectedArg, actualArg);
        }

        Map<String, String> aliases = builder.build();
        return match(actual.getBody(), expected.getBody(), aliases);
    }
}
