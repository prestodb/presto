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
package com.facebook.presto.orc.metadata.statistics;

import io.airlift.slice.Slice;

import static java.util.Objects.requireNonNull;

public class BinaryStatisticsBuilder
        implements SliceColumnStatisticsBuilder
{
    private long nonNullValueCount;

    @Override
    public void addValue(Slice value)
    {
        requireNonNull(value, "value is null");

        nonNullValueCount++;
    }

    @Override
    public ColumnStatistics buildColumnStatistics()
    {
        return new ColumnStatistics(
                nonNullValueCount,
                null,
                null,
                null,
                null,
                null,
                null,
                null);
    }
}
