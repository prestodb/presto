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

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorIndex;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordSet;

import java.util.List;

import static java.util.Objects.requireNonNull;

//FIXME : please check, do we really required this class? If yes, do it properly
public class KafkaConnectorIndex
        implements ConnectorIndex
{
    private final KafkaIndexHandle indexHandle;
    private final List<ColumnHandle> lookupColumns;
    private final List<ColumnHandle> outputColumns;
    private final KafkaMetadata restMetadata;
    private final ConnectorSession connectorSession;
    private final KafkaMetadataClient restMetadataClient;

    public KafkaConnectorIndex(KafkaIndexHandle indexHandle, List<ColumnHandle> lookupColumns,
            List<ColumnHandle> outputColumns, ConnectorSession connectorSession, KafkaMetadata restMetadata,
            KafkaMetadataClient restMetadataClient)
    {
        this.indexHandle = requireNonNull(indexHandle, "indexHandle is null");
        this.lookupColumns = requireNonNull(lookupColumns, "lookupColumns is null");
        this.outputColumns = requireNonNull(outputColumns, "outputColumns is null");
        this.connectorSession = requireNonNull(connectorSession, "connectorSession is null");
        this.restMetadata = requireNonNull(restMetadata, "restMetadata is null");
        this.restMetadataClient = requireNonNull(restMetadataClient, "restMetadataClient is null");
    }

    // FIXME : Indexing not required for now. Check later and remove if required.
    @Override
    public ConnectorPageSource lookup(RecordSet recordSet)
    {
        return null;
        // new RestIndexPageSource(indexHandle, lookupColumns,
        // outputColumns, recordSet, connectorSession,
        // restMetadata, restMetadataClient);
    }
}
