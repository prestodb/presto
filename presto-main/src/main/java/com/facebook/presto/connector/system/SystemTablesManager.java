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
