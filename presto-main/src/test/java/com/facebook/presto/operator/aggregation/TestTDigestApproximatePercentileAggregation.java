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

import com.facebook.presto.metadata.FunctionManager;
import com.facebook.presto.metadata.MetadataManager;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.tdigest.StatisticalDigestImplementation.TDIGEST;

public class TestTDigestApproximatePercentileAggregation
        extends TestStatisticalDigestApproximatePercentileAggregation
{
    private static final FunctionManager functionManager = MetadataManager.createTestMetadataManager(new FeaturesConfig().setStatisticalDigestImplementation(TDIGEST)).getFunctionManager();

    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION = getAggregation(functionManager, DOUBLE, DOUBLE);
    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION = getAggregation(functionManager, DOUBLE, BIGINT, DOUBLE);
    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_PARAMETER = getAggregation(functionManager, DOUBLE, BIGINT, DOUBLE, DOUBLE);

    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION = getAggregation(functionManager, DOUBLE, new ArrayType(DOUBLE));
    private static final InternalAggregationFunction DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED_AGGREGATION = getAggregation(functionManager, DOUBLE, BIGINT, new ArrayType(DOUBLE));

    private TestTDigestApproximatePercentileAggregation()
    {
        super(new FeaturesConfig().setStatisticalDigestImplementation(TDIGEST));
    }

    @Override
    protected InternalAggregationFunction getInternalAggregationFunction(int arity)
    {
        switch (arity) {
            case 2:
                return DOUBLE_APPROXIMATE_PERCENTILE_AGGREGATION;
            case 3:
                return DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION;
            case 4:
                return DOUBLE_APPROXIMATE_PERCENTILE_WEIGHTED_AGGREGATION_WITH_PARAMETER;
            default:
                throw new IllegalArgumentException("Unsupported number of arguments");
        }
    }

    @Override
    protected InternalAggregationFunction getArrayInternalAggregationFunction(int arity)
    {
        switch (arity) {
            case 2:
                return DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_AGGREGATION;
            case 3:
                return DOUBLE_APPROXIMATE_PERCENTILE_ARRAY_WEIGHTED_AGGREGATION;
            default:
                throw new IllegalArgumentException("Unsupported number of arguments");
        }
    }
}
