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
package io.prestosql.operator.aggregation.histogram;

import io.prestosql.spi.PrestoException;
import io.prestosql.spi.function.AccumulatorStateFactory;
import io.prestosql.spi.type.Type;

import static io.prestosql.operator.aggregation.histogram.HistogramGroupImplementation.LEGACY;
import static io.prestosql.operator.aggregation.histogram.HistogramGroupImplementation.NEW;
import static io.prestosql.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static java.lang.String.format;

public class HistogramStateFactory
        implements AccumulatorStateFactory<HistogramState>
{
    private final Type keyType;
    private final int expectedEntriesCount;
    private final HistogramGroupImplementation mode;

    public HistogramStateFactory(
            Type keyType, int
            expectedEntriesCount,
            HistogramGroupImplementation mode)
    {
        this.keyType = keyType;
        this.expectedEntriesCount = expectedEntriesCount;
        this.mode = mode;
    }

    @Override
    public HistogramState createSingleState()
    {
        return new SingleHistogramState(keyType, expectedEntriesCount);
    }

    @Override
    public Class<? extends HistogramState> getSingleStateClass()
    {
        return SingleHistogramState.class;
    }

    @Override
    public HistogramState createGroupedState()
    {
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
        if (mode == NEW) {
            return GroupedHistogramState.class;
        }

        if (mode == LEGACY) {
            return LegacyHistogramGroupState.class;
        }

        throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, format("expected group enum type %s", mode));
    }
}
