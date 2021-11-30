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
package com.facebook.presto.iceberg;

import com.google.common.collect.ImmutableList;
import org.apache.hadoop.conf.Configuration;
import org.apache.iceberg.BaseMetastoreCatalog;
import org.apache.iceberg.BaseMetastoreTableOperations;
import org.apache.iceberg.CatalogProperties;
import org.apache.iceberg.TableMetadata;
import org.apache.iceberg.TableOperations;
import org.apache.iceberg.catalog.Catalog;
import org.apache.iceberg.catalog.Namespace;
import org.apache.iceberg.catalog.SupportsNamespaces;
import org.apache.iceberg.catalog.TableIdentifier;
import org.apache.iceberg.exceptions.AlreadyExistsException;
import org.apache.iceberg.exceptions.CommitFailedException;
import org.apache.iceberg.exceptions.NamespaceNotEmptyException;
import org.apache.iceberg.exceptions.NoSuchNamespaceException;
import org.apache.iceberg.exceptions.NoSuchTableException;
import org.apache.iceberg.hadoop.HadoopFileIO;
import org.apache.iceberg.io.FileIO;

import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.ConcurrentHashMap;
import java.util.stream.Collectors;

import static java.util.Objects.requireNonNull;
import static org.apache.iceberg.CatalogProperties.WAREHOUSE_LOCATION;
import static org.apache.iceberg.CatalogUtil.dropTableData;
import static org.apache.iceberg.CatalogUtil.loadFileIO;

/**
 * An in memory catalog implementation to test Iceberg native mode
 */
public class InMemoryCatalog
        extends BaseMetastoreCatalog
        implements SupportsNamespaces, Catalog
{
    private final ConcurrentHashMap<Namespace, Map<String, String>> namespaces = new ConcurrentHashMap<>();
    private final ConcurrentHashMap<TableIdentifier, String> tables = new ConcurrentHashMap<>();

    private String warehouseLocation;
    private FileIO fileIO;

    @Override
    public String name()
    {
        return "memory";
    }

    @Override
    public void initialize(String name, Map<String, String> properties)
    {
        this.warehouseLocation = requireNonNull(properties.get(WAREHOUSE_LOCATION), "warehouseLocation is null");
        String fileIOImpl = properties.get(CatalogProperties.FILE_IO_IMPL);
        if (fileIOImpl == null) {
            this.fileIO = new HadoopFileIO(new Configuration());
            fileIO.initialize(properties);
        }
        else {
            this.fileIO = loadFileIO(fileIOImpl, properties, new Configuration());
        }
    }

    @Override
    public void createNamespace(Namespace namespace, Map<String, String> map)
    {
        if (namespaces.containsKey(namespace)) {
            throw new AlreadyExistsException("Namespace already exists: %s", namespace);
        }
        namespaces.put(namespace, map);
    }

    @Override
    public List<Namespace> listNamespaces(Namespace namespace)
            throws NoSuchNamespaceException
    {
        if (namespace.isEmpty()) {
            return ImmutableList.copyOf(namespaces.keySet());
        }
        validateNamespaceExists(namespace);
        return ImmutableList.of(namespace);
    }

    @Override
    public Map<String, String> loadNamespaceMetadata(Namespace namespace)
            throws NoSuchNamespaceException
    {
        validateNamespaceExists(namespace);
        return namespaces.get(namespace);
    }

    @Override
    public boolean dropNamespace(Namespace namespace)
            throws NamespaceNotEmptyException
    {
        validateNamespaceExists(namespace);
        if (!listTables(namespace).isEmpty()) {
            throw new NamespaceNotEmptyException("Namespace %s contains tables: %s", namespace, tables);
        }
        namespaces.remove(namespace);
        return true;
    }

    @Override
    public boolean setProperties(Namespace namespace, Map<String, String> map)
            throws NoSuchNamespaceException
    {
        validateNamespaceExists(namespace);
        namespaces.get(namespace).putAll(map);
        return true;
    }

    @Override
    public boolean removeProperties(Namespace namespace, Set<String> set)
            throws NoSuchNamespaceException
    {
        validateNamespaceExists(namespace);
        set.forEach(namespaces.get(namespace)::remove);
        return true;
    }

    @Override
    public boolean namespaceExists(Namespace namespace)
    {
        return namespaces.containsKey(namespace);
    }

    @Override
    public List<TableIdentifier> listTables(Namespace namespace)
    {
        if (namespace.isEmpty()) {
            return ImmutableList.copyOf(tables.keySet());
        }
        validateNamespaceExists(namespace);
        return tables.keySet().stream()
                .filter(id -> id.hasNamespace() && id.namespace().equals(namespace))
                .collect(Collectors.toList());
    }

    @Override
    protected TableOperations newTableOps(TableIdentifier tableIdentifier)
    {
        return new InMemoryTableOperations(fileIO, tableIdentifier);
    }

    @Override
    protected String defaultWarehouseLocation(TableIdentifier tableIdentifier)
    {
        String tableName = tableIdentifier.name();
        StringBuilder sb = new StringBuilder();

        sb.append(warehouseLocation).append('/');
        for (String level : tableIdentifier.namespace().levels()) {
            sb.append(level).append('/');
        }
        sb.append(tableName);
        return sb.toString();
    }

    @Override
    public boolean dropTable(TableIdentifier tableIdentifier, boolean purge)
    {
        validateTableExists(tableIdentifier);
        TableOperations ops = newTableOps(tableIdentifier);
        TableMetadata lastMetadata = ops.current();
        tables.remove(tableIdentifier);
        if (purge) {
            dropTableData(fileIO, lastMetadata);
        }
        return true;
    }

    @Override
    public void renameTable(TableIdentifier from, TableIdentifier to)
    {
        validateTableExists(from);
        if (tables.containsKey(to)) {
            throw new AlreadyExistsException("Cannot rename %s to %s: %s already exists", from, to, to);
        }
        tables.put(to, tables.remove(from));
    }

    private void validateNamespaceExists(Namespace namespace)
    {
        if (!namespaces.containsKey(namespace)) {
            throw new NoSuchNamespaceException("Namespace does not exist: %s", namespace);
        }
    }

    private void validateTableExists(TableIdentifier tableIdentifier)
    {
        if (!tables.containsKey(tableIdentifier)) {
            throw new NoSuchTableException("Table does not exist: %s", tableIdentifier);
        }
    }

    public class InMemoryTableOperations
            extends BaseMetastoreTableOperations
    {
        private final FileIO fileIO;
        private final TableIdentifier tableIdentifier;

        public InMemoryTableOperations(FileIO fileIO, TableIdentifier tableIdentifier)
        {
            this.fileIO = fileIO;
            this.tableIdentifier = tableIdentifier;
        }

        @Override
        protected String tableName()
        {
            return tableIdentifier.toString();
        }

        @Override
        public FileIO io()
        {
            return fileIO;
        }

        @Override
        protected void doRefresh()
        {
            String metadataLocation = null;
            if (tables.containsKey(tableIdentifier)) {
                metadataLocation = tables.get(tableIdentifier);
            }
            else {
                if (currentMetadataLocation() != null) {
                    throw new NoSuchTableException("Cannot find Glue table %s after refresh, " +
                            "maybe another process deleted it or revoked your access permission", tableName());
                }
            }
            refreshFromMetadataLocation(metadataLocation);
        }

        @Override
        protected void doCommit(TableMetadata base, TableMetadata metadata)
        {
            String newMetadataLocation = writeNewMetadata(metadata, currentVersion() + 1);
            synchronized (tables) {
                String inMemoryLocation = tables.getOrDefault(tableIdentifier, null);
                String baseLocation = base != null ? base.metadataFileLocation() : null;
                if (!Objects.equals(inMemoryLocation, baseLocation)) {
                    throw new CommitFailedException(
                            "Cannot commit %s because base metadata location '%s' is not same as the current location '%s' in catalog",
                            tableName(), baseLocation, inMemoryLocation);
                }
                tables.put(tableIdentifier, newMetadataLocation);
            }
        }
    }
}
