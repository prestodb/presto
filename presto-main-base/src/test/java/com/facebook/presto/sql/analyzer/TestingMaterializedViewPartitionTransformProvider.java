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

import com.facebook.presto.spi.function.FunctionMetadata;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionRange;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransform;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformArgument;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformProperties;
import com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformProvider;
import com.facebook.presto.spi.relation.ConstantExpression;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformProperties.MappingCardinality.IDENTITY;
import static com.facebook.presto.spi.materializedview.MaterializedViewPartitionTransformProperties.MappingCardinality.MANY_TO_ONE;
import static io.airlift.slice.Slices.utf8Slice;
import static java.util.Objects.requireNonNull;

public final class TestingMaterializedViewPartitionTransformProvider
        implements MaterializedViewPartitionTransformProvider
{
    public static final String FUNCTION_NAME = "test_partition_bucket";
    private static final String MODE_PROPERTY = "mode";

    @Override
    public Set<String> getTransformIds()
    {
        return ImmutableSet.of(FUNCTION_NAME);
    }

    @Override
    public Set<String> getFunctionNames()
    {
        return ImmutableSet.of(FUNCTION_NAME);
    }

    @Override
    public Optional<MaterializedViewPartitionTransform> analyze(
            FunctionMetadata function,
            List<MaterializedViewPartitionTransformArgument> arguments)
    {
        if (arguments.size() != 2 || !arguments.get(0).isSourceColumn() || !arguments.get(1).getConstant().isPresent()) {
            throw new IllegalArgumentException(FUNCTION_NAME + " requires one source column and one constant mode");
        }
        return Optional.of(new TestingTransform(readMode(arguments.get(1).getConstant().get())));
    }

    @Override
    public MaterializedViewPartitionTransform decode(Map<String, String> properties)
    {
        requireNonNull(properties, "properties is null");
        return new TestingTransform(requireMode(properties.get(MODE_PROPERTY)));
    }

    private static String readMode(ConstantExpression expression)
    {
        if (expression.getType().getJavaType() != Slice.class || expression.isNull()) {
            throw new IllegalArgumentException("mode must be a non-null varchar");
        }
        return requireMode(((Slice) expression.getValue()).toStringUtf8());
    }

    private static String requireMode(String mode)
    {
        if (!"identity".equals(mode) && !"many".equals(mode)) {
            throw new IllegalArgumentException("Unsupported test partition transform mode: " + mode);
        }
        return mode;
    }

    private static final class TestingTransform
            implements MaterializedViewPartitionTransform
    {
        private final String mode;

        private TestingTransform(String mode)
        {
            this.mode = mode;
        }

        @Override
        public String getTransformId()
        {
            return FUNCTION_NAME;
        }

        @Override
        public MaterializedViewPartitionTransformProperties getProperties()
        {
            return new MaterializedViewPartitionTransformProperties(
                    0,
                    ImmutableSet.of(1),
                    "identity".equals(mode) ? IDENTITY : MANY_TO_ONE,
                    1);
        }

        @Override
        public Map<String, String> encode()
        {
            return ImmutableMap.of("transform", FUNCTION_NAME, MODE_PROPERTY, mode);
        }

        @Override
        public boolean isCanonicalTargetValue(ConstantExpression value)
        {
            return value.getType().getJavaType() == Slice.class && !value.isNull() && !((Slice) value.getValue()).toStringUtf8().contains(".");
        }

        @Override
        public ConstantExpression apply(ConstantExpression sourceValue)
        {
            if ("identity".equals(mode) || sourceValue.isNull()) {
                return sourceValue;
            }
            String value = ((Slice) sourceValue.getValue()).toStringUtf8();
            int separator = value.indexOf('.');
            String bucket = separator < 0 ? value : value.substring(0, separator);
            return new ConstantExpression(Optional.empty(), utf8Slice(bucket), VARCHAR);
        }

        @Override
        public MaterializedViewPartitionRange getSourceRange(ConstantExpression targetValue)
        {
            if (!isCanonicalTargetValue(targetValue)) {
                throw new IllegalArgumentException("target value is not canonical");
            }
            String value = ((Slice) targetValue.getValue()).toStringUtf8();
            return new MaterializedViewPartitionRange(
                    new ConstantExpression(Optional.empty(), utf8Slice(value + ".0"), VARCHAR),
                    new ConstantExpression(Optional.empty(), utf8Slice(value + ".9"), VARCHAR));
        }
    }
}
