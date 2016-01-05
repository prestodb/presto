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
package com.facebook.presto.hive;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;

import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;

public class HiveBucketHandle
{
    private final List<HiveColumnHandle> columns;
    private final int bucketCount;

    @JsonCreator
    public HiveBucketHandle(@JsonProperty("columns") List<HiveColumnHandle> columns, @JsonProperty("bucketCount") int bucketCount)
    {
        this.columns = requireNonNull(columns, "columns is null");
        this.bucketCount = requireNonNull(bucketCount, "bucketCount is null");
    }

    @JsonProperty
    public List<HiveColumnHandle> getColumns()
    {
        return columns;
    }

    @JsonProperty
    public int getBucketCount()
    {
        return bucketCount;
    }

    public HiveBucketProperty toBucketProperty()
    {
        return new HiveBucketProperty(
                columns.stream()
                        .map(HiveColumnHandle::getName)
                        .collect(toList()),
                bucketCount);
    }
}
