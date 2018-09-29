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

package com.facebook.presto.kafkastream;

import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class KafkaRecordSet
        implements RecordSet
{
    private final List<KafkaColumnHandle> columnHandles;
    private final List<ColumnMetadata> columnMetadatas;
    private final List<Type> columnTypes;
    private final ConnectorSession connectorSession;
    private final KafkaSplit split;
    private final KafkaClient client;

    public KafkaRecordSet(KafkaSplit split,
            List<KafkaColumnHandle> columnHandles,
            List<ColumnMetadata> columnMetadatas,
            ConnectorSession connectorSession,
            KafkaClient client)
    {
        this.split = requireNonNull(split, "split is null");
        this.columnHandles = requireNonNull(columnHandles, "column handles is null");
        ImmutableList.Builder<Type> types = ImmutableList.builder();
        for (KafkaColumnHandle column : columnHandles) {
            types.add(column.getColumnType());
        }
        this.columnTypes = types.build();
        this.columnMetadatas = requireNonNull(columnMetadatas, "columnMetadatas is null");
        this.connectorSession =
                requireNonNull(connectorSession, "connectorSession is null");
        this.client = client;
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new KafkaRecordCursor(
                split,
                columnHandles,
                columnMetadatas,
                connectorSession,
                client);
    }
}
