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
package com.facebook.presto.accumulo;

import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.accumulo.conf.AccumuloSessionProperties;
import com.facebook.presto.accumulo.conf.AccumuloTableProperties;
import com.facebook.presto.accumulo.index.IndexLookup;
import com.facebook.presto.accumulo.index.Indexer;
import com.facebook.presto.accumulo.io.AccumuloPageSink;
import com.facebook.presto.accumulo.metadata.AccumuloMetadataManager;
import com.facebook.presto.accumulo.metadata.AccumuloTable;
import com.facebook.presto.accumulo.metadata.AccumuloView;
import com.facebook.presto.accumulo.model.AccumuloColumnConstraint;
import com.facebook.presto.accumulo.model.AccumuloColumnHandle;
import com.facebook.presto.accumulo.model.TabletSplitMetadata;
import com.facebook.presto.accumulo.serializers.AccumuloRowSerializer;
import com.facebook.presto.spi.ColumnMetadata;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.ConnectorTableMetadata;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.facebook.presto.spi.predicate.Domain;
import com.facebook.presto.spi.predicate.Marker.Bound;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.Instance;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.Scanner;
import org.apache.accumulo.core.client.ZooKeeperInstance;
import org.apache.accumulo.core.client.security.tokens.PasswordToken;
import org.apache.accumulo.core.data.Key;
import org.apache.accumulo.core.data.PartialKey;
import org.apache.accumulo.core.data.Range;
import org.apache.accumulo.core.data.Value;
import org.apache.accumulo.core.security.Authorizations;
import org.apache.accumulo.minicluster.MiniAccumuloCluster;
import org.apache.commons.io.FileUtils;
import org.apache.commons.lang3.tuple.Pair;
import org.apache.hadoop.io.Text;

import javax.inject.Inject;

import java.io.File;
import java.io.IOException;
import java.nio.file.Files;
import java.security.InvalidParameterException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.HashMap;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Map.Entry;
import java.util.Optional;
import java.util.Random;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_DNE;
import static com.facebook.presto.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_EXISTS;
import static com.facebook.presto.accumulo.AccumuloErrorCode.COLUMN_NOT_FOUND;
import static com.facebook.presto.accumulo.AccumuloErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.accumulo.AccumuloErrorCode.MINI_ACCUMULO;
import static com.facebook.presto.accumulo.AccumuloErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static com.facebook.presto.accumulo.AccumuloErrorCode.VALIDATION;
import static com.facebook.presto.accumulo.AccumuloErrorCode.VIEW_ALREADY_EXISTS;
import static com.facebook.presto.accumulo.AccumuloErrorCode.VIEW_IS_TABLE;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

/**
 * This class is the main access point for the Presto connector to interact with Accumulo. It is
 * responsible for creating tables, dropping tables, retrieving table metadata, and getting the
 * ConnectorSplits from a table.
 * <p>
 * Classes that requires an Accumulo Connector object should use the static method to retrieve it.
 * This function will create a MiniAccumuloCluster connection for testing, or a 'real' one for production use.
 */
public class AccumuloClient
{
    private static final Logger LOG = Logger.get(AccumuloClient.class);
    private static final String MAC_PASSWORD = "secret";
    private static final String MAC_USER = "root";
    private static final Splitter COMMA_SPLITTER = Splitter.on(',').omitEmptyStrings().trimResults();

    private static Connector conn = null;

    private final AccumuloConfig conf;
    private final AccumuloMetadataManager metaManager;
    private final Authorizations auths;
    private final Random random = new Random();
    private final AccumuloTableManager tableManager;
    private final IndexLookup sIndexLookup;

    /**
     * Gets an Accumulo Connector based on the given configuration.
     * Will start/stop MiniAccumuloCluster should that be true in the config.
     *
     * @param config Accumulo configuration
     * @return Accumulo connector
     */
    public static synchronized Connector getAccumuloConnector(AccumuloConfig config)
    {
        if (conn != null) {
            return conn;
        }

        requireNonNull(config, "config is null");

        try {
            if (config.isMiniAccumuloCluster()) {
                MiniAccumuloCluster accumulo = createMiniAccumuloCluster();
                Instance inst = new ZooKeeperInstance(accumulo.getInstanceName(), accumulo.getZooKeepers());
                conn = inst.getConnector(MAC_USER, new PasswordToken(MAC_PASSWORD));
                LOG.info("Connection to MAC instance %s at %s established, user %s password %s",
                        accumulo.getInstanceName(), accumulo.getZooKeepers(), MAC_USER, MAC_PASSWORD);
            }
            else {
                Instance inst = new ZooKeeperInstance(config.getInstance(), config.getZooKeepers());
                conn = inst.getConnector(config.getUsername(),
                        new PasswordToken(config.getPassword().getBytes()));
                LOG.info("Connection to instance %s at %s established, user %s", config.getInstance(),
                        config.getZooKeepers(), config.getUsername());
            }
        }
        catch (AccumuloException | AccumuloSecurityException | InterruptedException | IOException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to get conn to Accumulo", e);
        }

        return conn;
    }

    /**
     * Creates and starts an instance of MiniAccumuloCluster, returning the new instance
     *
     * @return New MiniAccumuloCluster
     * @throws IOException
     * @throws InterruptedException
     */
    private static MiniAccumuloCluster createMiniAccumuloCluster()
            throws IOException, InterruptedException
    {
        // Create MAC directory
        File macDir = Files.createTempDirectory("mac-").toFile();
        LOG.info("MAC is enabled, starting MiniAccumuloCluster at %s", macDir);

        // Start MAC and connect to it
        MiniAccumuloCluster accumulo = new MiniAccumuloCluster(macDir, MAC_PASSWORD);
        accumulo.start();

        // Add shutdown hook to stop MAC and cleanup temporary files
        Runtime.getRuntime().addShutdownHook(new Thread(() -> {
            try {
                LOG.info("Shutting down MAC");
                accumulo.stop();
            }
            catch (IOException | InterruptedException e) {
                Thread.currentThread().interrupt();
                throw new PrestoException(MINI_ACCUMULO, "Failed to shut down MAC instance", e);
            }

            try {
                LOG.info("Cleaning up MAC directory");
                FileUtils.forceDelete(macDir);
            }
            catch (IOException e) {
                throw new PrestoException(MINI_ACCUMULO, "Failed to clean up MAC directory", e);
            }
        }));

        return accumulo;
    }

    /**
     * Creates a new instance of an AccumuloClient, injected by that Guice. Creates a connection to
     * Accumulo.
     *
     * @param config Connector configuration for Accumulo
     * @throws AccumuloException If an Accumulo error occurs
     * @throws AccumuloSecurityException If Accumulo credentials are not valid
     */
    @Inject
    public AccumuloClient(AccumuloConfig config)
            throws AccumuloException, AccumuloSecurityException, IOException, InterruptedException
    {
        this.conf = requireNonNull(config, "config is null");
        this.metaManager = config.getMetadataManager();
        this.tableManager = new AccumuloTableManager(config);

        // Creates and sets CONNECTOR in the event it has not yet been created
        this.auths = getAccumuloConnector(this.conf).securityOperations().getUserAuthorizations(conf.getUsername());

        // Create the index lookup utility
        this.sIndexLookup = new IndexLookup(conn, conf, this.auths);
    }

    /**
     * Creates a new AccumuloTable based on the given metadata.
     *
     * @param meta Metadata for the table
     * @return {@link AccumuloTable} for the newly created table
     */
    public AccumuloTable createTable(ConnectorTableMetadata meta)
    {
        // Validate the DDL is something we can handle
        validateCreateTable(meta);

        Map<String, Object> tableProperties = meta.getProperties();
        String rowIdColumn = getRowIdColumn(meta);

        // Get the list of column handles
        List<AccumuloColumnHandle> columns = getColumnHandles(meta, rowIdColumn);

        // Create the AccumuloTable object
        AccumuloTable table = new AccumuloTable(meta.getTable().getSchemaName(),
                meta.getTable().getTableName(), columns, rowIdColumn,
                AccumuloTableProperties.isExternal(tableProperties),
                AccumuloTableProperties.getSerializerClass(tableProperties),
                AccumuloTableProperties.getScanAuthorizations(tableProperties));

        // First, create the metadata
        metaManager.createTableMetadata(table);

        // Make sure the namespace exists
        tableManager.ensureNamespace(table.getSchema());

        // Create the Accumulo table if it does not exist (for 'external' table)
        if (!tableManager.exists(table.getFullTableName())) {
            tableManager.createAccumuloTable(table.getFullTableName());
        }

        // Set any locality groups on the data table
        setLocalityGroups(tableProperties, table);

        // Create index tables, if appropriate
        createIndexTables(table);

        return table;
    }

    /**
     * Validates the given metadata for a series of conditions to ensure the table is well-formed
     *
     * @param meta Table metadata
     */
    private void validateCreateTable(ConnectorTableMetadata meta)
    {
        validateColumns(meta);
        validateLocalityGroups(meta);
        if (!AccumuloTableProperties.isExternal(meta.getProperties())) {
            validateInternalTable(meta);
        }
    }

    /**
     * Validates the columns set in the configure locality groups (if any) exist
     *
     * @param meta Table metadata
     */
    private void validateColumns(ConnectorTableMetadata meta)
    {
        // Check all the column types, and throw an exception if the types of a map are complex
        // While it is a rare case, this is not supported by the Accumulo connector
        ImmutableSet.Builder<String> columnNameBuilder = ImmutableSet.builder();
        for (ColumnMetadata column : meta.getColumns()) {
            if (Types.isMapType(column.getType())) {
                if (Types.isMapType(Types.getKeyType(column.getType()))
                        || Types.isMapType(Types.getValueType(column.getType()))
                        || Types.isArrayType(Types.getKeyType(column.getType()))
                        || Types.isArrayType(Types.getValueType(column.getType()))) {
                    throw new PrestoException(VALIDATION,
                            "Key/value types of a MAP column must be plain types");
                }
            }

            columnNameBuilder.add(column.getName().toLowerCase(Locale.ENGLISH));
        }

        // Validate the columns are distinct
        if (columnNameBuilder.build().size() != meta.getColumns().size()) {
            throw new PrestoException(VALIDATION, "Duplicate column names are not supported");
        }

        Optional<Map<String, Pair<String, String>>> columnMapping = AccumuloTableProperties.getColumnMapping(meta.getProperties());
        if (columnMapping.isPresent()) {
            // Validate there are no duplicates in the column mapping
            long distinctMappings = columnMapping.get().values().stream().distinct().count();
            if (distinctMappings != columnMapping.get().size()) {
                throw new PrestoException(VALIDATION,
                        "Duplicate column family/qualifier pair detected in column mapping, check the value of " +
                                AccumuloTableProperties.COLUMN_MAPPING);
            }

            // Validate no column is mapped to the reserved entry
            String reservedRowIdColumn = AccumuloPageSink.ROW_ID_COLUMN.toString();
            if (columnMapping.get().values().stream()
                    .filter(pair -> pair.getKey().equals(reservedRowIdColumn)
                            && pair.getValue().equals(reservedRowIdColumn)).count() > 0) {
                throw new PrestoException(VALIDATION,
                        format("Column familiy/qualifier mapping of %s:%s is reserved",
                                reservedRowIdColumn, reservedRowIdColumn));
            }
        }
        else if (AccumuloTableProperties.isExternal(meta.getProperties())) {
            // Column mapping is not defined (i.e. use column generation) and table is external
            // But column generation is for internal tables only
            throw new PrestoException(VALIDATION,
                    "Column generation for external tables is not supported, must specify " +
                            AccumuloTableProperties.COLUMN_MAPPING);
        }
    }

    /**
     * Validates the columns set in the configure locality groups (if any) exist
     *
     * @param meta Table metadata
     */
    private void validateLocalityGroups(ConnectorTableMetadata meta)
    {
        // Validate any configured locality groups
        Optional<Map<String, Set<String>>> groups =
                AccumuloTableProperties.getLocalityGroups(meta.getProperties());
        if (!groups.isPresent()) {
            return;
        }

        String rowIdColumn = getRowIdColumn(meta);

        // For each locality group
        for (Map.Entry<String, Set<String>> g : groups.get().entrySet()) {
            if (g.getValue().contains(rowIdColumn)) {
                throw new PrestoException(VALIDATION, "Row ID column cannot be in a locality group");
            }

            // Validate the specified column names exist in the table definition,
            // incrementing a counter for each matching column
            int matchingColumns = 0;
            for (ColumnMetadata column : meta.getColumns()) {
                if (g.getValue().contains(column.getName().toLowerCase(Locale.ENGLISH))) {
                    ++matchingColumns;

                    // Break out early if all columns are found
                    if (matchingColumns == g.getValue().size()) {
                        break;
                    }
                }
            }

            // If the number of matched columns does not equal the defined size,
            // then a column was specified that does not exist
            // (or there is a duplicate column in the table DDL, which is also an issue but
            // has been checked before in validateColumns)
            if (matchingColumns != g.getValue().size()) {
                throw new PrestoException(VALIDATION, format("Unknown Presto column defined for locality group %s", g.getKey()));
            }
        }
    }

    /**
     * Validates the Accumulo table (and index tables, if applicable) do not already exist, if internal
     *
     * @param meta Table metadata
     */
    private void validateInternalTable(ConnectorTableMetadata meta)
    {
        String table = AccumuloTable.getFullTableName(meta.getTable());
        String indexTable = Indexer.getIndexTableName(meta.getTable());
        String metricsTable = Indexer.getMetricsTableName(meta.getTable());

        if (conn.tableOperations().exists(table)) {
            throw new PrestoException(ACCUMULO_TABLE_EXISTS,
                    "Cannot create internal table when an Accumulo table already exists");
        }

        if (AccumuloTableProperties.getIndexColumns(meta.getProperties()).isPresent()) {
            if (conn.tableOperations().exists(indexTable) || conn.tableOperations().exists(metricsTable)) {
                throw new PrestoException(ACCUMULO_TABLE_EXISTS,
                        "Internal table is indexed, but the index table and/or index metrics table(s) already exist");
            }
        }
    }

    /**
     * Gets the row ID based on a table properties or the first column name
     *
     * @param meta ConnectorTableMetadata
     * @return Lowercase Presto column name mapped to the Accumulo row ID
     */
    private String getRowIdColumn(ConnectorTableMetadata meta)
    {
        Optional<String> rowIdColumn = AccumuloTableProperties.getRowId(meta.getProperties());
        return rowIdColumn.orElse(meta.getColumns().get(0).getName()).toLowerCase(Locale.ENGLISH);
    }

    private List<AccumuloColumnHandle> getColumnHandles(ConnectorTableMetadata meta, String rowIdColumn)
    {
        // Get the column mappings from the table property or auto-generate columns if not defined
        Map<String, Pair<String, String>> mapping =
                AccumuloTableProperties.getColumnMapping(meta.getProperties())
                        .orElse(autoGenerateMapping(meta.getColumns(), AccumuloTableProperties.getLocalityGroups(meta.getProperties())));

        // The list of indexed columns
        Optional<List<String>> indexedColumns = AccumuloTableProperties.getIndexColumns(meta.getProperties());

        // And now we parse the configured columns and create handles for the
        // metadata manager
        ImmutableList.Builder<AccumuloColumnHandle> cBuilder = ImmutableList.builder();
        for (int ordinal = 0; ordinal < meta.getColumns().size(); ++ordinal) {
            ColumnMetadata cm = meta.getColumns().get(ordinal);

            // Special case if this column is the
            if (cm.getName().equalsIgnoreCase(rowIdColumn)) {
                cBuilder.add(new AccumuloColumnHandle(rowIdColumn, Optional.empty(), Optional.empty(),
                        cm.getType(), ordinal, "Accumulo row ID", false));
            }
            else {
                if (!mapping.containsKey(cm.getName())) {
                    throw new InvalidParameterException(String
                            .format("Misconfigured mapping for presto column %s", cm.getName()));
                }

                // Get the mapping for this column
                Pair<String, String> famqual = mapping.get(cm.getName());
                boolean indexed = indexedColumns.isPresent() && indexedColumns.get().contains(cm.getName().toLowerCase(Locale.ENGLISH));
                String comment = String.format("Accumulo column %s:%s. Indexed: %b",
                        famqual.getLeft(), famqual.getRight(), indexed);

                // Create a new AccumuloColumnHandle object
                cBuilder.add(new AccumuloColumnHandle(cm.getName(), Optional.of(famqual.getLeft()),
                        Optional.of(famqual.getRight()), cm.getType(), ordinal, comment, indexed));
            }
        }

        return cBuilder.build();
    }

    /**
     * Extracts the locality groups from the given tasble properties and sets them for the Accumulo table
     *
     * @param tableProperties Table properties
     * @param table AccumuloTable object
     */
    private void setLocalityGroups(Map<String, Object> tableProperties, AccumuloTable table)
    {
        Optional<Map<String, Set<String>>> groups = AccumuloTableProperties.getLocalityGroups(tableProperties);
        if (!groups.isPresent()) {
            LOG.info("No locality groups to set");
            return;
        }

        ImmutableMap.Builder<String, Set<Text>> localityGroupsBldr = ImmutableMap.builder();
        for (Map.Entry<String, Set<String>> g : groups.get().entrySet()) {
            ImmutableSet.Builder<Text> famBldr = ImmutableSet.builder();
            // For each configured column for this locality group
            for (String col : g.getValue()) {
                // Locate the column family mapping via the Handle
                // We already validated this earlier, so it'll exist
                famBldr.add(new Text(table.getColumns().stream()
                        .filter(x -> x.getName().equals(col)).collect(Collectors.toList())
                        .get(0).getFamily().get()));
            }
            localityGroupsBldr.put(g.getKey(), famBldr.build());
        }

        Map<String, Set<Text>> localityGroups = localityGroupsBldr.build();
        LOG.info("Setting locality groups: {}", localityGroups);
        tableManager.setLocalityGroups(table.getFullTableName(), localityGroups);
    }

    /**
     * Creates the index tables from the given Accumulo table. No op if
     * {@link AccumuloTable#isIndexed()} is false.
     *
     * @param table Table to create index tables
     */
    private void createIndexTables(AccumuloTable table)
    {
        // Early-out if table is not indexed
        if (!table.isIndexed()) {
            return;
        }

        // Create index table if it does not exist (for 'external' table)
        if (!tableManager.exists(table.getIndexTableName())) {
            tableManager.createAccumuloTable(table.getIndexTableName());
        }

        // Create index metrics table if it does not exist
        if (!tableManager.exists(table.getMetricsTableName())) {
            tableManager.createAccumuloTable(table.getMetricsTableName());
        }

        // Set locality groups on index and metrics table
        Map<String, Set<Text>> indexGroups = Indexer.getLocalityGroups(table);
        tableManager.setLocalityGroups(table.getIndexTableName(), indexGroups);
        tableManager.setLocalityGroups(table.getMetricsTableName(), indexGroups);

        // Attach iterators to metrics table
        for (IteratorSetting s : Indexer.getMetricIterators(table)) {
            tableManager.setIterator(table.getMetricsTableName(), s);
        }
    }

    /**
     * Auto-generates the mapping of Presto column name to Accumulo family/qualifier, respecting the locality groups (if any).
     *
     * @param columns Presto columns for the table
     * @param groups Mapping of locality groups to a set of Presto columns, or null if none
     * @return Column mappings
     */
    private Map<String, Pair<String, String>> autoGenerateMapping(List<ColumnMetadata> columns, Optional<Map<String, Set<String>>> groups)
    {
        Map<String, Pair<String, String>> mapping = new HashMap<>();
        for (ColumnMetadata column : columns) {
            Optional<String> fam = getColumnLocalityGroup(column.getName(), groups);
            mapping.put(column.getName(), Pair.of(fam.orElse(column.getName()), column.getName()));
        }
        return mapping;
    }

    /**
     * Searches through the given locality groups to find if this column has a locality group.
     *
     * @param columnName Column name to get the locality group of
     * @param groups Optional locality group configuration
     * @return Optional string containing the name of the locality group, if present
     */
    private Optional<String> getColumnLocalityGroup(String columnName, Optional<Map<String, Set<String>>> groups)
    {
        if (groups.isPresent()) {
            for (Map.Entry<String, Set<String>> g : groups.get().entrySet()) {
                if (g.getValue().contains(columnName.toLowerCase(Locale.ENGLISH))) {
                    return Optional.of(g.getKey());
                }
            }
        }

        return Optional.empty();
    }

    /**
     * Drops the table metadata from Presto and Accumulo tables if internal table
     * Checks for metadata and table existence before deleting
     *
     * @param table The Accumulo table
     */
    public void dropTable(AccumuloTable table)
    {
        SchemaTableName stName = new SchemaTableName(table.getSchema(), table.getTable());
        String tableName = table.getFullTableName();

        // Drop cardinality cache from index lookup
        sIndexLookup.dropCache(stName.getSchemaName(), stName.getTableName());

        // Remove the table metadata from Presto
        if (metaManager.getTable(stName) != null) {
            metaManager.deleteTableMetadata(stName);
        }

        if (!table.isExternal()) {
            // delete the table and index tables
            if (tableManager.exists(tableName)) {
                tableManager.deleteAccumuloTable(tableName);
            }

            if (table.isIndexed()) {
                String indexTableName = Indexer.getIndexTableName(stName);
                if (tableManager.exists(indexTableName)) {
                    tableManager.deleteAccumuloTable(indexTableName);
                }

                String metricsTableName = Indexer.getMetricsTableName(stName);
                if (tableManager.exists(metricsTableName)) {
                    tableManager.deleteAccumuloTable(metricsTableName);
                }
            }
        }
    }

    /**
     * Renames the metadata associated Accumulo table
     *
     * @param oldName Old table name
     * @param newName New table name
     */
    public void renameTable(SchemaTableName oldName, SchemaTableName newName)
    {
        if (!oldName.getSchemaName().equals(newName.getSchemaName())) {
            throw new PrestoException(NOT_SUPPORTED,
                    "Accumulo does not support renaming tables to different namespaces (schemas)");
        }

        AccumuloTable oldTable = getTable(oldName);
        if (oldTable == null) {
            throw new TableNotFoundException(oldName);
        }

        AccumuloTable newTable = oldTable.clone();
        newTable.setTable(newName.getTableName());

        // Validate table existence
        if (!tableManager.exists(oldTable.getFullTableName())) {
            throw new PrestoException(ACCUMULO_TABLE_DNE, format("Table %s does not exist", oldTable.getFullTableName()));
        }

        if (tableManager.exists(newTable.getFullTableName())) {
            throw new PrestoException(ACCUMULO_TABLE_EXISTS, format("Table %s already exists", newTable.getFullTableName()));
        }

        // Rename index tables (which will also validate table existence)
        renameIndexTables(oldTable, newTable);

        // Rename the Accumulo table
        tableManager.renameAccumuloTable(oldTable.getFullTableName(), newTable.getFullTableName());

        // We'll then create the metadata
        metaManager.deleteTableMetadata(oldTable.getSchemaTableName());
        metaManager.createTableMetadata(newTable);
    }

    /**
     * Renames the index tables, if applicable, of from the old table to the new table
     *
     * @param oldTable Old AccumuloTable
     * @param newTable New AccumuloTable
     */
    private void renameIndexTables(AccumuloTable oldTable, AccumuloTable newTable)
    {
        if (!oldTable.isIndexed()) {
            return;
        }

        if (!tableManager.exists(oldTable.getIndexTableName())) {
            throw new PrestoException(ACCUMULO_TABLE_DNE, format("Table %s does not exist", oldTable.getIndexTableName()));
        }

        if (tableManager.exists(newTable.getIndexTableName())) {
            throw new PrestoException(ACCUMULO_TABLE_EXISTS, format("Table %s already exists", newTable.getIndexTableName()));
        }

        if (!tableManager.exists(oldTable.getMetricsTableName())) {
            throw new PrestoException(ACCUMULO_TABLE_DNE, format("Table %s does not exist", oldTable.getMetricsTableName()));
        }

        if (tableManager.exists(newTable.getMetricsTableName())) {
            throw new PrestoException(ACCUMULO_TABLE_EXISTS, format("Table %s already exists", newTable.getMetricsTableName()));
        }

        tableManager.renameAccumuloTable(oldTable.getIndexTableName(), newTable.getIndexTableName());
        tableManager.renameAccumuloTable(oldTable.getMetricsTableName(), newTable.getMetricsTableName());
    }

    /**
     * Creates a new view with the given name and data.
     *
     * @param viewName Name of the view
     * @param viewData Data for the view
     */
    public void createView(SchemaTableName viewName, String viewData)
    {
        if (getSchemaNames().contains(viewName.getSchemaName())) {
            if (getViewNames(viewName.getSchemaName()).contains(viewName.getTableName())) {
                throw new PrestoException(VIEW_ALREADY_EXISTS, "View already exists");
            }

            if (getTableNames(viewName.getSchemaName()).contains(viewName.getTableName())) {
                throw new PrestoException(VIEW_IS_TABLE, "View already exists as data table");
            }
        }

        metaManager.createViewMetadata(new AccumuloView(viewName.getSchemaName(), viewName.getTableName(), viewData));
    }

    /**
     * Creates a new view with the given name and data.  Replaces a view if 'replace' is true.
     *
     * @param viewName Name of the view
     * @param viewData Data for the view
     */
    public void createOrReplaceView(SchemaTableName viewName, String viewData)
    {
        if (getView(viewName) != null) {
            metaManager.deleteViewMetadata(viewName);
        }

        metaManager.createViewMetadata(new AccumuloView(viewName.getSchemaName(), viewName.getTableName(), viewData));
    }

    /**
     * Drops the given view
     *
     * @param viewName View to drop
     */
    public void dropView(SchemaTableName viewName)
    {
        metaManager.deleteViewMetadata(viewName);
    }

    /**
     * Rename the column of an existing table
     *
     * @param table Accumulo table to act on
     * @param source Existing column name
     * @param target New column name
     */
    public void renameColumn(AccumuloTable table, String source, String target)
    {
        if (table.getRowId().equalsIgnoreCase(source)) {
            table.setRowId(target);
        }

        // Locate the column to rename
        for (AccumuloColumnHandle col : table.getColumns()) {
            if (col.getName().equalsIgnoreCase(source)) {
                // Rename the column
                col.setName(target);

                // Recreate the table metadata with the new name and exit
                metaManager.deleteTableMetadata(
                        new SchemaTableName(table.getSchema(), table.getTable()));
                metaManager.createTableMetadata(table);
                return;
            }
        }

        throw new PrestoException(COLUMN_NOT_FOUND,
                format("Failed to find source column %s to rename to %s", source, target));
    }

    /**
     * Gets all schema names via the {@link AccumuloMetadataManager}
     *
     * @return The set of schema names
     */
    public Set<String> getSchemaNames()
    {
        return metaManager.getSchemaNames();
    }

    /**
     * Gets all table names from the given schema
     *
     * @param schema The schema to get table names from
     * @return The set of table names
     */
    public Set<String> getTableNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        return metaManager.getTableNames(schema);
    }

    /**
     * Gets the {@link AccumuloTable} for the given name via the {@link AccumuloMetadataManager}
     *
     * @param table The table to fetch
     * @return The AccumuloTable or null if it does not exist
     */
    public AccumuloTable getTable(SchemaTableName table)
    {
        requireNonNull(table, "schema table name is null");
        return metaManager.getTable(table);
    }

    /**
     * Gets all view names from the given schema
     *
     * @param schema The schema to get table names from
     * @return The set of table names
     */
    public Set<String> getViewNames(String schema)
    {
        requireNonNull(schema, "schema is null");
        return metaManager.getViewNames(schema);
    }

    /**
     * Gets the {@link AccumuloView} for the given name via the {@link AccumuloMetadataManager}
     *
     * @param viewName The view to fetch
     * @return The AccumuloView or null if it does not exist
     */
    public AccumuloView getView(SchemaTableName viewName)
    {
        requireNonNull(viewName, "schema table name is null");
        return metaManager.getView(viewName);
    }

    /**
     * Fetches the TabletSplitMetadata for a query against an Accumulo table. Does a whole bunch of
     * fun stuff! Such as splitting on row ID ranges, applying secondary indexes, column pruning,
     * all sorts of sweet optimizations. What you have here is an important method.
     *
     * @param session Current session
     * @param schema Schema name
     * @param table Table Name
     * @param rowIdDom Domain for the row ID
     * @param constraints Column constraints for the query
     * @param serializer Instance of a row serializer
     * @return List of TabletSplitMetadata objects for Presto
     */
    public List<TabletSplitMetadata> getTabletSplits(ConnectorSession session, String schema,
            String table, Optional<Domain> rowIdDom, List<AccumuloColumnConstraint> constraints,
            AccumuloRowSerializer serializer)
    {
        try {
            String tableName = AccumuloTable.getFullTableName(schema, table);
            LOG.info("Getting tablet splits for table %s", tableName);

            // Get the initial Range based on the row ID domain
            Collection<Range> rowIdRanges = getRangesFromDomain(rowIdDom, serializer);
            List<TabletSplitMetadata> tabletSplits = new ArrayList<>();

            // Use the secondary index, if enabled
            if (AccumuloSessionProperties.isOptimizeIndexEnabled(session)) {
                // Get the scan authorizations to query the index and set them in our lookup utility
                sIndexLookup.setAuths(getScanAuthorizations(session, schema, table));

                // Check the secondary index based on the column constraints
                // If this returns true, return the tablet splits to Presto
                if (sIndexLookup.applyIndex(schema, table, session, constraints, rowIdRanges,
                        tabletSplits, serializer)) {
                    return tabletSplits;
                }
            }

            // If we can't (or shouldn't) use the secondary index,
            // we will just use the Range from the row ID domain

            // Split the ranges on tablet boundaries, if enabled
            Collection<Range> splitRanges;
            if (AccumuloSessionProperties.isOptimizeSplitRangesEnabled(session)) {
                splitRanges = splitByTabletBoundaries(tableName, rowIdRanges);
            }
            else {
                // if not enabled, just use the same collection
                splitRanges = rowIdRanges;
            }

            // Create TabletSplitMetadata objects for each range
            boolean fetchTabletLocations =
                    AccumuloSessionProperties.isOptimizeLocalityEnabled(session);

            LOG.info("Fetching tablet locations: %s", fetchTabletLocations);

            for (Range r : splitRanges) {
                // If locality is enabled, then fetch tablet location
                if (fetchTabletLocations) {
                    tabletSplits.add(new TabletSplitMetadata(
                            getTabletLocation(tableName, r.getStartKey()), ImmutableList.of(r)));
                }
                else {
                    // else, just use the default location
                    tabletSplits.add(new TabletSplitMetadata(Optional.empty(), ImmutableList.of(r)));
                }
            }

            // Log some fun stuff and return the tablet splits
            LOG.info("Number of splits for table %s is %d with %d ranges", tableName,
                    tabletSplits.size(), splitRanges.size());
            return tabletSplits;
        }
        catch (Exception e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR,
                    "Failed to get splits from Accumulo", e);
        }
    }

    /**
     * Gets the scan authorizations to use for scanning tables.<br>
     * <br>
     * In order of priority: session username authorizations, then table property, then the default
     * connector auths
     *
     * @param session Current session
     * @param schema Schema name
     * @param table Table Name
     * @return Scan authorizations
     * @throws AccumuloException If a generic Accumulo error occurs
     * @throws AccumuloSecurityException If a security exception occurs
     */
    private Authorizations getScanAuthorizations(ConnectorSession session, String schema,
            String table)
            throws AccumuloException, AccumuloSecurityException
    {
        String sessionScanUser = AccumuloSessionProperties.getScanUsername(session);
        if (sessionScanUser != null) {
            Authorizations scanAuths =
                    conn.securityOperations().getUserAuthorizations(sessionScanUser);
            LOG.info("Using session scan auths for user %s: %s", sessionScanUser, scanAuths);
            return scanAuths;
        }

        AccumuloTable accumuloTable = this.getTable(new SchemaTableName(schema, table));
        if (accumuloTable == null) {
            throw new TableNotFoundException(new SchemaTableName(schema, table));
        }

        Optional<String> strAuths = accumuloTable.getScanAuthorizations();
        if (strAuths.isPresent()) {
            Authorizations scanAuths = new Authorizations(Iterables.toArray(COMMA_SPLITTER.split(strAuths.get()), String.class));
            LOG.info("scan_auths table property set, using: %s", scanAuths);
            return scanAuths;
        }

        LOG.info("scan_auths table property not set, using connector auths: %s", this.auths);
        return this.auths;
    }

    /**
     * Splits the given collection of ranges based on tablet boundaries, returning a new collection
     * of ranges
     *
     * @param tableName Fully-qualified table name
     * @param ranges The collection of Ranges to split
     * @return A new collection of Ranges split on tablet boundaries
     * @throws TableNotFoundException
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     */
    private Collection<Range> splitByTabletBoundaries(String tableName, Collection<Range> ranges)
            throws org.apache.accumulo.core.client.TableNotFoundException, AccumuloException, AccumuloSecurityException
    {
        ImmutableSet.Builder<Range> rangeBuilder = ImmutableSet.builder();
        for (Range r : ranges) {
            // if start and end key are equivalent, no need to split the range
            if (r.getStartKey() != null && r.getEndKey() != null
                    && r.getStartKey().equals(r.getEndKey())) {
                rangeBuilder.add(r);
            }
            else {
                // Call out to Accumulo to split the range on tablets
                rangeBuilder.addAll(conn.tableOperations().splitRangeByTablets(tableName, r,
                        Integer.MAX_VALUE));
            }
        }
        return rangeBuilder.build();
    }

    /**
     * Gets the TabletServer hostname for where the given key is located in the given table
     *
     * @param table Fully-qualified table name
     * @param key Key to locate
     * @return The tablet location, or DUMMY_LOCATION if an error occurs
     */
    private Optional<String> getTabletLocation(String table, Key key)
    {
        try {
            // Get the Accumulo table ID so we can scan some fun stuff
            String tableId = conn.tableOperations().tableIdMap().get(table);

            // Create our scanner against the metadata table, fetching 'loc' family
            Scanner scan = conn.createScanner("accumulo.metadata", auths);
            scan.fetchColumnFamily(new Text("loc"));

            // Set the scan range to just this table, from the table ID to the default tablet
            // row, which is the last listed tablet
            Key defaultTabletRow = new Key(tableId + '<');
            Key start = new Key(tableId);
            Key end = defaultTabletRow.followingKey(PartialKey.ROW);
            scan.setRange(new Range(start, end));

            Optional<String> location = Optional.empty();
            if (key == null) {
                // if the key is null, then it is -inf, so get first tablet location
                for (Entry<Key, Value> kvp : scan) {
                    location = Optional.of(kvp.getValue().toString());
                    break;
                }
            }
            else {
                // Else, we will need to scan through the tablet location data and find the location

                // Create some text objects to do comparison for what we are looking for
                Text splitCompareKey = new Text();
                key.getRow(splitCompareKey);
                Text scannedCompareKey = new Text();

                // Scan the table!
                for (Entry<Key, Value> kvp : scan) {
                    // Get the bytes of the key
                    byte[] keyBytes = kvp.getKey().getRow().copyBytes();

                    // If the last byte is <, then we have hit the default tablet, so use this location
                    if (keyBytes[keyBytes.length - 1] == '<') {
                        location = Optional.of(kvp.getValue().toString());
                        break;
                    }
                    else {
                        // Chop off some magic nonsense
                        scannedCompareKey.set(keyBytes, 3, keyBytes.length - 3);

                        // Compare the keys, moving along the tablets until the location is found
                        if (scannedCompareKey.getLength() > 0) {
                            int compareTo = splitCompareKey.compareTo(scannedCompareKey);
                            if (compareTo <= 0) {
                                location = Optional.of(kvp.getValue().toString());
                            }
                            else {
                                // all future tablets will be greater than this key
                                break;
                            }
                        }
                    }
                }
                scan.close();
            }

            // If we were unable to find the location for some reason, return the default tablet
            // location
            return location.isPresent() ? location : getDefaultTabletLocation(table);
        }
        catch (Exception e) {
            // Swallow this exception so the query does not fail due to being unable
            // to locate the tablet server for the provided Key.
            // This is purely an optimization, but we will want to log the error.
            LOG.error("Failed to get tablet location, returning dummy location", e);
            e.printStackTrace();
            return Optional.empty();
        }
    }

    /**
     * Gets the location of the default tablet for the given table
     *
     * @param fulltable Fully-qualified table name
     * @return TabletServer location of the default tablet
     */
    private Optional<String> getDefaultTabletLocation(String fulltable)
    {
        try {
            // Get the table ID
            String tableId = conn.tableOperations().tableIdMap().get(fulltable);

            // Create a scanner over the metadata table, fetching the 'loc' column of the default
            // tablet row
            Scanner scan = conn.createScanner("accumulo.metadata",
                    conn.securityOperations().getUserAuthorizations(conf.getUsername()));
            scan.fetchColumnFamily(new Text("loc"));
            scan.setRange(new Range(tableId + '<'));

            // scan the entry
            Optional<String> location = Optional.empty();
            for (Entry<Key, Value> kvp : scan) {
                if (location != null) {
                    throw new PrestoException(INTERNAL_ERROR,
                            "Scan for default tablet returned more than one entry");
                }

                location = Optional.of(kvp.getValue().toString());
            }

            scan.close();
            return location;
        }
        catch (Exception e) {
            // Swallow this exception so the query does not fail due to being unable
            // to locate the tablet server for the default tablet.
            // This is purely an optimization, but we will want to log the error.
            LOG.error("Failed to get tablet location, returning dummy location", e);
            e.printStackTrace();
            return Optional.empty();
        }
    }

    /**
     * Gets a collection of Accumulo Range objects from the given Presto domain. This maps the
     * column constraints of the given Domain to an Accumulo Range scan.
     *
     * @param dom Domain, can be null (returns (-inf, +inf) Range)
     * @param serializer Instance of an {@link AccumuloRowSerializer}
     * @return A collection of Accumulo Range objects
     * @throws AccumuloException If an Accumulo error occurs
     * @throws AccumuloSecurityException If Accumulo credentials are not valid
     * @throws TableNotFoundException If the Accumulo table is not found
     */
    public static Collection<Range> getRangesFromDomain(Optional<Domain> dom, AccumuloRowSerializer serializer)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException
    {
        // if we have no predicate pushdown, use the full range
        if (!dom.isPresent()) {
            return ImmutableSet.of(new Range());
        }

        ImmutableSet.Builder<Range> rangeBuilder = ImmutableSet.builder();
        for (com.facebook.presto.spi.predicate.Range r : dom.get().getValues().getRanges()
                .getOrderedRanges()) {
            rangeBuilder.add(getRangeFromPrestoRange(r, serializer));
        }

        return rangeBuilder.build();
    }

    /**
     * Convert the given Presto range to an Accumulo range
     *
     * @param pRange Presto range
     * @param serializer Instance of an {@link AccumuloRowSerializer}
     * @return Accumulo range
     * @throws AccumuloException
     * @throws AccumuloSecurityException
     * @throws TableNotFoundException
     */
    private static Range getRangeFromPrestoRange(com.facebook.presto.spi.predicate.Range pRange,
            AccumuloRowSerializer serializer)
            throws AccumuloException, AccumuloSecurityException, TableNotFoundException
    {
        Range aRange;
        if (pRange.isAll()) {
            aRange = new Range();
        }
        else if (pRange.isSingleValue()) {
            Text split = new Text(serializer.encode(pRange.getType(), pRange.getSingleValue()));
            aRange = new Range(split);
        }
        else {
            if (pRange.getLow().isLowerUnbounded()) {
                // If low is unbounded, then create a range from (-inf, value), checking
                // inclusivity
                boolean inclusive = pRange.getHigh().getBound() == Bound.EXACTLY;
                Text split =
                        new Text(serializer.encode(pRange.getType(), pRange.getHigh().getValue()));
                aRange = new Range(null, false, split, inclusive);
            }
            else if (pRange.getHigh().isUpperUnbounded()) {
                // If high is unbounded, then create a range from (value, +inf), checking
                // inclusivity
                boolean inclusive = pRange.getLow().getBound() == Bound.EXACTLY;
                Text split =
                        new Text(serializer.encode(pRange.getType(), pRange.getLow().getValue()));
                aRange = new Range(split, inclusive, null, false);
            }
            else {
                // If high is unbounded, then create a range from low to high, checking
                // inclusivity
                boolean startKeyInclusive = pRange.getLow().getBound() == Bound.EXACTLY;
                Text startSplit =
                        new Text(serializer.encode(pRange.getType(), pRange.getLow().getValue()));

                boolean endKeyInclusive = pRange.getHigh().getBound() == Bound.EXACTLY;
                Text endSplit =
                        new Text(serializer.encode(pRange.getType(), pRange.getHigh().getValue()));
                aRange = new Range(startSplit, startKeyInclusive, endSplit, endKeyInclusive);
            }
        }

        return aRange;
    }
}
