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
package com.facebook.presto.operator.aggregation.state;

import com.facebook.presto.spi.function.AccumulatorStateFactory;
import com.facebook.presto.spi.type.Type;

public class HistogramStateFactory
        implements AccumulatorStateFactory<HistogramState>
{
    private final boolean fullyShared;
    private final Type keyType;
    private final int expectedEntriesCount;

    public HistogramStateFactory(Type keyType, int expectedEntriesCount)
    {
        this.keyType = keyType;
        this.expectedEntriesCount = expectedEntriesCount;
        // todo: hack: remove before merge and use config system and specialize if need be
        fullyShared = Boolean.parseBoolean(System.getProperty("presto.aggregations.grouped-histo-gram.fully-shared", "true"));
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
        if (fullyShared) {
            return new SingleInstanceGroupedHistogramState(keyType, expectedEntriesCount);
        }
        else {
            return new GroupedHistogramState(keyType, expectedEntriesCount);
        }
    }

    @Override
    public Class<? extends HistogramState> getGroupedStateClass()
    {
        if (fullyShared) {
            return SingleInstanceGroupedHistogramState.class;
        }
        else {
            return GroupedHistogramState.class;
        }
    }
}
