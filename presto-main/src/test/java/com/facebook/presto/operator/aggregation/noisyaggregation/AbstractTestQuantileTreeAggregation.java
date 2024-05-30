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
package com.facebook.presto.operator.aggregation.noisyaggregation;

import com.facebook.presto.common.type.SqlVarbinary;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.metadata.FunctionAndTypeManager;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.RandomizationStrategy;
import com.facebook.presto.operator.aggregation.noisyaggregation.sketch.quantiletree.QuantileTree;
import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.spi.function.JavaAggregationFunctionImplementation;

import java.util.function.BiFunction;

import static com.facebook.presto.sql.analyzer.TypeSignatureProvider.fromTypes;
import static io.airlift.slice.Slices.wrappedBuffer;

/**
 * For tests of QuantileTree-related aggregators (noisy_approx_percentile_qtree, noisy_qtree_agg, merge)
 */
public abstract class AbstractTestQuantileTreeAggregation
        extends AbstractTestFunctions
{
    protected static boolean quantileTreesMatch(Object input, Object refObj, double tolerance)
    {
        QuantileTree inputTree = parseQuantileTree(input);
        QuantileTree refTree = parseQuantileTree(refObj);
        for (double p : new double[] {0.1, 0.2, 0.3, 0.4, 0.5, 0.6, 0.7, 0.8, 0.9}) {
            if (Math.abs(inputTree.quantile(p) - refTree.quantile(p)) > tolerance) {
                return false;
            }
        }

        return true;
    }

    protected static BiFunction<Object, Object, Boolean> getQuantileComparator(double tolerance)
    {
        return (x, y) -> quantileTreesMatch(x, y, tolerance);
    }

    protected static QuantileTree parseQuantileTree(Object serialized)
    {
        return QuantileTree.deserialize(wrappedBuffer(((SqlVarbinary) serialized).getBytes()));
    }

    protected static Double[] generateNormal(int n, double mean, double standardDeviation, RandomizationStrategy randomizationStrategy)
    {
        Double[] values = new Double[n];
        for (int i = 0; i < n; i++) {
            values[i] = mean + standardDeviation * randomizationStrategy.nextGaussian();
        }
        return values;
    }

    protected static Double[] generateSequence(int n)
    {
        Double[] values = new Double[n];
        for (int i = 0; i < n; i++) {
            values[i] = (double) i;
        }
        return values;
    }

    protected JavaAggregationFunctionImplementation getFunction(String name, Type... type)
    {
        FunctionAndTypeManager functionAndTypeManager = functionAssertions.getMetadata().getFunctionAndTypeManager();
        return functionAssertions.getMetadata().getFunctionAndTypeManager().getJavaAggregateFunctionImplementation(
                functionAndTypeManager.lookupFunction(name, fromTypes(type)));
    }
}
