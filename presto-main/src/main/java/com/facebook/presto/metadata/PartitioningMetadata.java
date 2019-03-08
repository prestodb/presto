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
package com.facebook.presto.metadata;

import com.facebook.presto.sql.planner.PartitioningHandle;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class PartitioningMetadata
{
    private final PartitioningHandle partitioningHandle;
    private final List<String> partitionColumns;

    public PartitioningMetadata(PartitioningHandle partitioningHandle, List<String> partitionColumns)
    {
        this.partitioningHandle = requireNonNull(partitioningHandle, "partitioningHandle is null");
        this.partitionColumns = ImmutableList.copyOf(requireNonNull(partitionColumns, "partitionColumns is null"));
    }

    public PartitioningHandle getPartitioningHandle()
    {
        return partitioningHandle;
    }

    public List<String> getPartitionColumns()
    {
        return partitionColumns;
    }
}
