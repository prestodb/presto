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
package com.facebook.presto.operator.aggregation;

import com.facebook.presto.metadata.BoundVariables;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarcharType;
import com.facebook.presto.type.TypeRegistry;
import com.google.common.collect.ImmutableList;

import io.airlift.slice.Slices;

import java.util.concurrent.ThreadLocalRandom;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.testing.AggregationTestUtils.generateInternalAggregationFunction;

public class TestApproximateCountDistinctVarBinary
        extends AbstractTestApproximateCountDistinct
{
    public static final InternalAggregationFunction VARBINARY_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS =
      generateInternalAggregationFunction(
              ApproximateCountDistinctAggregations.class,
              BIGINT.getTypeSignature(),
              ImmutableList.of(VarcharType.getParametrizedVarcharSignature("x"), DOUBLE.getTypeSignature()),
              new TypeRegistry(), BoundVariables.builder().setLongVariable("x", (long) Integer.MAX_VALUE).build(), 1);

    @Override
    public InternalAggregationFunction getAggregationFunction()
    {
        return VARBINARY_APPROXIMATE_COUNT_DISTINCT_AGGREGATIONS;
    }

    @Override
    public Type getValueType()
    {
        return VarcharType.VARCHAR;
    }

    @Override
    public Object randomValue()
    {
        int length = ThreadLocalRandom.current().nextInt(100);
        byte[] bytes = new byte[length];
        ThreadLocalRandom.current().nextBytes(bytes);

        return Slices.wrappedBuffer(bytes);
    }
}
