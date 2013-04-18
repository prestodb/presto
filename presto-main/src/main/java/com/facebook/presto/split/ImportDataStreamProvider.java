package com.facebook.presto.split;

import com.facebook.presto.ingest.RecordProjectOperator;
import com.facebook.presto.operator.Operator;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ImportClient;
import com.facebook.presto.spi.Split;

import javax.inject.Inject;
import java.util.List;

import static com.google.common.base.Preconditions.checkNotNull;

public class ImportDataStreamProvider
        implements ConnectorDataStreamProvider
{
    private final ImportClient importClient;

    @Inject
    public ImportDataStreamProvider(ImportClient importClient)
    {
        this.importClient = checkNotNull(importClient, "importClient is null");
    }

    @Override
    public boolean canHandle(Split split)
    {
        return importClient.canHandle(split);
    }

    @Override
    public Operator createDataStream(Split split, List<ColumnHandle> columns)
    {
        checkNotNull(split, "split is null");
        checkNotNull(columns, "columns is null");

        return new RecordProjectOperator(importClient.getRecords(split, columns));
    }
}
