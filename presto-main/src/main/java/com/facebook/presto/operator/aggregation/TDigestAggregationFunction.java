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

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.operator.aggregation.state.StatisticalDigestState;

import static com.facebook.presto.common.type.StandardTypes.TDIGEST;
import static com.facebook.presto.common.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.operator.aggregation.StatisticalDigestFactory.createStatisticalTDigest;
import static com.facebook.presto.operator.aggregation.state.StatisticalDigestStateFactory.createTDigestFactory;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.facebook.presto.spi.function.SqlFunctionVisibility.EXPERIMENTAL;
import static com.facebook.presto.tdigest.TDigest.createTDigest;
import static com.facebook.presto.util.Failures.checkCondition;

public class TDigestAggregationFunction
        extends StatisticalDigestAggregationFunction
{
    public static final TDigestAggregationFunction TDIGEST_AGG = new TDigestAggregationFunction(parseTypeSignature("V"));
    public static final TDigestAggregationFunction TDIGEST_AGG_WITH_WEIGHT = new TDigestAggregationFunction(parseTypeSignature("V"), parseTypeSignature(StandardTypes.BIGINT));
    public static final TDigestAggregationFunction TDIGEST_AGG_WITH_WEIGHT_AND_COMPRESSION = new TDigestAggregationFunction(
            parseTypeSignature("V"),
            parseTypeSignature(StandardTypes.BIGINT),
            parseTypeSignature(StandardTypes.DOUBLE));

    public static final String NAME = "tdigest_agg";

    private TDigestAggregationFunction(TypeSignature... typeSignatures)
    {
        super(NAME, TDIGEST, createTDigestFactory(), EXPERIMENTAL, typeSignatures);
    }

    @Override
    public String getDescription()
    {
        return "Returns a tdigest from the set of doubles";
    }

    public static void inputDouble(StatisticalDigestState state, double value, long weight, double compression)
    {
        checkCondition(weight > 0, INVALID_FUNCTION_ARGUMENT, "Weight must be > 0, was %s", weight);
        checkCondition(compression > 0, INVALID_FUNCTION_ARGUMENT, "Compression factor must be positive, was %s", compression);

        StatisticalDigest digest = getOrCreateTDigest(state, compression);
        state.addMemoryUsage(-digest.estimatedInMemorySizeInBytes());
        digest.add(value, weight);
        state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
    }

    private static StatisticalDigest getOrCreateTDigest(StatisticalDigestState state, double compression)
    {
        StatisticalDigest digest = state.getStatisticalDigest();
        if (digest == null) {
            digest = createStatisticalTDigest(createTDigest(compression));
            state.setStatisticalDigest(digest);
            state.addMemoryUsage(digest.estimatedInMemorySizeInBytes());
        }
        return digest;
    }
}
