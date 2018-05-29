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
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;

import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class ElasticsearchRecordSet
        implements RecordSet
{
    private final List<ElasticsearchColumnHandle> columnHandles;
    private final List<Type> columnTypes;
    private final ElasticsearchSplit split;
    private final ElasticsearchConnectorConfig config;

    public ElasticsearchRecordSet(ElasticsearchSplit split, ElasticsearchConnectorConfig config, List<ElasticsearchColumnHandle> columnHandles)
    {
        this.split = requireNonNull(split, "split is null");
        this.config = requireNonNull(config, "config is null");
        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");
        this.columnTypes = columnHandles.stream()
                .map(ElasticsearchColumnHandle::getColumnType)
                .collect(toImmutableList());
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new ElasticsearchRecordCursor(columnHandles, config, split);
    }
}
