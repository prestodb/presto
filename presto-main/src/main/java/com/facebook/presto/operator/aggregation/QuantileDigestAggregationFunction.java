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

import com.facebook.presto.operator.aggregation.state.StatisticalDigestState;
import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.TypeSignature;
import io.airlift.stats.QuantileDigest;

import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.doubleToSortableLong;
import static com.facebook.presto.operator.aggregation.FloatingPointBitsConverterUtil.floatToSortableInt;
import static com.facebook.presto.operator.aggregation.state.StatisticalDigestStateFactory.createQuantileDigestFactory;
import static com.facebook.presto.spi.type.StandardTypes.QDIGEST;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.Float.intBitsToFloat;

public final class QuantileDigestAggregationFunction
        extends StatisticalDigestAggregationFunction
{
    public static final QuantileDigestAggregationFunction QDIGEST_AGG = new QuantileDigestAggregationFunction(parseTypeSignature("V"));
    public static final QuantileDigestAggregationFunction QDIGEST_AGG_WITH_WEIGHT = new QuantileDigestAggregationFunction(
            parseTypeSignature("V"),
            parseTypeSignature(StandardTypes.BIGINT));
    public static final QuantileDigestAggregationFunction QDIGEST_AGG_WITH_WEIGHT_AND_ERROR = new QuantileDigestAggregationFunction(
            parseTypeSignature("V"),
            parseTypeSignature(StandardTypes.BIGINT),
            parseTypeSignature(StandardTypes.DOUBLE));

    public static final String NAME = "qdigest_agg";

    private QuantileDigestAggregationFunction(TypeSignature... typeSignatures)
    {
        super(NAME, QDIGEST, createQuantileDigestFactory(), false, typeSignatures);
    }

    @Override
    public String getDescription()
    {
        return "Returns a qdigest from the set of reals, bigints, doubles";
    }

    public static void inputDouble(StatisticalDigestState state, double value, long weight, double parameter)
    {
        inputBigint(state, doubleToSortableLong(value), weight, parameter);
    }

    public static void inputReal(StatisticalDigestState state, long value, long weight, double accuracy)
    {
        inputBigint(state, floatToSortableInt(intBitsToFloat((int) value)), weight, accuracy);
    }

    public static void inputBigint(StatisticalDigestState state, long value, long weight, double accuracy)
    {
        checkArgument(accuracy > 0 && accuracy < 1, "Percentile accuracy must be exclusively between 0 and 1, was %s", accuracy);
        checkArgument(weight > 0, "Weight must be positive, was %s", weight);
        StatisticalDigest digest = getOrCreateQuantileDigest(state, accuracy);
        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value, weight);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
    }

    private static StatisticalDigest getOrCreateQuantileDigest(StatisticalDigestState state, double parameter)
    {
        StatisticalDigest digest = state.getStatisticalDigest();
        if (digest == null) {
            digest = new StatisticalQuantileDigest(new QuantileDigest(parameter));
            state.setStatisticalDigest(digest);
            state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
        }
        return digest;
    }
}
