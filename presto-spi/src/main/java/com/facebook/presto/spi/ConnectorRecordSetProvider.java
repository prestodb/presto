package com.facebook.presto.spi;

import java.util.List;

public interface ConnectorRecordSetProvider
{
    boolean canHandle(Split split);

    RecordSet getRecordSet(Split split, List<? extends ColumnHandle> columns);
}
