package com.facebook.presto.connector.system;

import com.facebook.presto.spi.SystemTable;

import javax.inject.Inject;
import java.util.Set;

//
// This class exists only to eliminate circular dependencies.
// For example, the QuerySystemTable exposes information about queries in the QueryManager, and since
// the QueryManager depends on Metadata.  Metadata, in turn, depends on the SystemTablesMetadata which
// depends on the QuerySystemTable.  Finally, QuerySystemTable needs access to the QueryManager which
// is a circular dependency.
// This class allows the following construction to be created and linked:
//   QuerySystemTable -> QueryManager -> Metadata -> SystemTablesMetadata
// Then this class adds the QuerySystemTable to the SystemTablesMetadata.
//
public class SystemTablesManager
{
    private final SystemTablesMetadata metadata;
    private final SystemSplitManager splitManager;
    private final SystemDataStreamProvider dataStreamProvider;

    @Inject
    public SystemTablesManager(SystemTablesMetadata metadata, SystemSplitManager splitManager, SystemDataStreamProvider dataStreamProvider, Set<SystemTable> tables)
    {
        this.metadata = metadata;
        this.splitManager = splitManager;
        this.dataStreamProvider = dataStreamProvider;
        for (SystemTable table : tables) {
            addTable(table);
        }
    }

    public void addTable(SystemTable systemTable)
    {
        metadata.addTable(systemTable.getTableMetadata());
        splitManager.addTable(systemTable);
        dataStreamProvider.addTable(systemTable);
    }
}
