
package com.facebook.presto.elasticsearch;

import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorRecordSetProvider;
import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;

import javax.inject.Inject;

import java.util.List;

import static com.facebook.presto.elasticsearch.Types.checkType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;

public class ElasticsearchRecordSetProvider
        implements ConnectorRecordSetProvider
{
    private final String connectorId;

    @Inject
    public ElasticsearchRecordSetProvider(ElasticsearchConnectorId connectorId)
    {
        this.connectorId = checkNotNull(connectorId, "connectorId is null").toString();
    }

    @Override
    public RecordSet getRecordSet(ConnectorSplit split, List<? extends ColumnHandle> columns)
    {
        checkNotNull(split, "partitionChunk is null");
        ElasticsearchSplit elasticsearchSplit = checkType(split, ElasticsearchSplit.class, "split");
        checkArgument(elasticsearchSplit.getConnectorId().equals(connectorId), "split is not for this connector");

        ImmutableList.Builder<ElasticsearchColumnHandle> handles = ImmutableList.builder();
        for (ColumnHandle handle : columns) {
            handles.add(checkType(handle, ElasticsearchColumnHandle.class, "handle"));
        }

        return new ElasticsearchRecordSet(elasticsearchSplit, handles.build());
    }
}
