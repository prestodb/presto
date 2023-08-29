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
package com.facebook.presto.operator.aggregation.histogram;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.operator.aggregation.estimatendv.GroupNDVEstimatorState;
import com.facebook.presto.operator.aggregation.estimatendv.SingleNDVEstimatorState;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.AccumulatorStateFactory;

import static com.facebook.presto.operator.aggregation.histogram.HistogramGroupImplementation.LEGACY;
import static com.facebook.presto.operator.aggregation.histogram.HistogramGroupImplementation.NEW;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static java.lang.String.format;

public class HistogramStateFactory
        implements AccumulatorStateFactory<HistogramState>
{
    private final Type keyType;
    private final int expectedEntriesCount;
    private final HistogramGroupImplementation mode;
    private boolean ndvEstimate;

    public HistogramStateFactory(
            Type keyType, int
            expectedEntriesCount,
            HistogramGroupImplementation mode)
    {
        this.keyType = keyType;
        this.expectedEntriesCount = expectedEntriesCount;
        this.mode = mode;
    }

    public HistogramStateFactory(
            Type keyType, int
            expectedEntriesCount,
            HistogramGroupImplementation mode,
            boolean ndvEstimate)
    {
        this(keyType, expectedEntriesCount, mode);
        this.ndvEstimate = ndvEstimate;
    }

    @Override
    public HistogramState createSingleState()
    {
        if (ndvEstimate) {
            return new SingleNDVEstimatorState(keyType, expectedEntriesCount);
        }
        return new SingleHistogramState(keyType, expectedEntriesCount);
    }

    @Override
    public Class<? extends HistogramState> getSingleStateClass()
    {
        if (ndvEstimate) {
            return SingleNDVEstimatorState.class;
        }
        return SingleHistogramState.class;
    }

    @Override
    public HistogramState createGroupedState()
    {
        if (ndvEstimate) {
            return new GroupNDVEstimatorState(keyType, expectedEntriesCount);
        }

        if (mode == NEW) {
            return new GroupedHistogramState(keyType, expectedEntriesCount);
        }

        if (mode == LEGACY) {
            return new LegacyHistogramGroupState(keyType, expectedEntriesCount);
        }

        throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, format("expected group enum type %s", mode));
    }

    @Override
    public Class<? extends HistogramState> getGroupedStateClass()
    {
        if (ndvEstimate) {
            return GroupNDVEstimatorState.class;
        }
        if (mode == NEW) {
            return GroupedHistogramState.class;
        }

        if (mode == LEGACY) {
            return LegacyHistogramGroupState.class;
        }

        throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, format("expected group enum type %s", mode));
    }
}
