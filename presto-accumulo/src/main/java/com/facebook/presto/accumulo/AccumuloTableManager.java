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
import com.facebook.presto.spi.PrestoException;
import io.airlift.log.Logger;
import org.apache.accumulo.core.client.AccumuloException;
import org.apache.accumulo.core.client.AccumuloSecurityException;
import org.apache.accumulo.core.client.Connector;
import org.apache.accumulo.core.client.IteratorSetting;
import org.apache.accumulo.core.client.NamespaceExistsException;
import org.apache.accumulo.core.client.TableExistsException;
import org.apache.accumulo.core.client.TableNotFoundException;
import org.apache.accumulo.core.iterators.IteratorUtil.IteratorScope;
import org.apache.hadoop.io.Text;

import java.util.EnumSet;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_DNE;
import static com.facebook.presto.accumulo.AccumuloErrorCode.ACCUMULO_TABLE_EXISTS;
import static com.facebook.presto.accumulo.AccumuloErrorCode.UNEXPECTED_ACCUMULO_ERROR;
import static java.util.Objects.requireNonNull;

/**
 * This class is a light wrapper for Accumulo's Connector object.
 * It will perform the given operation, or throw an exception if an
 * Accumulo- or ZooKeeper-based error occurs.
 */
public class AccumuloTableManager
{
    private static final Logger LOG = Logger.get(AccumuloTableManager.class);
    private static final String DEFAULT = "default";
    private final Connector connector;

    public AccumuloTableManager(AccumuloConfig config)
    {
        this.connector = AccumuloClient.getAccumuloConnector(requireNonNull(config, "config is null"));
    }

    /**
     * Ensures the given Accumulo namespace exist, creating it if necessary
     *
     * @param schema Presto schema (Accumulo namespace)
     */
    public void ensureNamespace(String schema)
    {
        try {
            // If the table schema is not "default" and the namespace does not exist, create it
            if (!schema.equals(DEFAULT)
                    && !connector.namespaceOperations().exists(schema)) {
                connector.namespaceOperations().create(schema);
            }
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to check for existence or create Accumulo namespace", e);
        }
        catch (NamespaceExistsException e) {
            // Suppress race condition between test for existence and creation
            LOG.warn("NamespaceExistsException suppressed when creating " + schema);
        }
    }

    /**
     * Gets a Boolean value indicating whether or not the given table exists
     *
     * @param table Table to check for existence
     * @return True if it exists, false otherwise
     */
    public boolean exists(String table)
    {
        return connector.tableOperations().exists(table);
    }

    /**
     * Creates the given Accumulo table
     *
     * @param table Accumulo table to create, including namespace (if not default)
     */
    public void createAccumuloTable(String table)
    {
        try {
            connector.tableOperations().create(table);
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to create Accumulo table", e);
        }
        catch (TableExistsException e) {
            throw new PrestoException(ACCUMULO_TABLE_EXISTS, "Accumulo table already exists", e);
        }
    }

    /**
     * Sets the locality groups to the given Accumulo table
     *
     * @param tableName Accumulo table name
     * @param groups Locality groups
     */
    public void setLocalityGroups(String tableName, Map<String, Set<Text>> groups)
    {
        if (groups.size() == 0) {
            return;
        }

        try {
            connector.tableOperations().setLocalityGroups(tableName, groups);
            LOG.info("Set locality groups for %s to %s", tableName, groups);
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to set locality groups", e);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(ACCUMULO_TABLE_DNE, "Failed to set locality groups, table does not exist", e);
        }
    }

    /**
     * Sets the settings to the given table, first removing any existing iterator of the same name
     *
     * @param table Accumulo table
     * @param setting Settings to set
     */
    public void setIterator(String table, IteratorSetting setting)
    {
        try {
            // Remove any existing iterator settings of the same name, if applicable
            Map<String, EnumSet<IteratorScope>> iterators = connector.tableOperations().listIterators(table);
            if (iterators.containsKey(setting.getName())) {
                connector.tableOperations().removeIterator(table, setting.getName(), iterators.get(setting.getName()));
            }

            connector.tableOperations().attachIterator(table, setting);
        }
        catch (AccumuloSecurityException | AccumuloException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to set iterator on table " + table, e);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(ACCUMULO_TABLE_DNE, "Failed to set iterator, table does not exist", e);
        }
    }

    /**
     * Deletes the Accumulo table
     *
     * @param tableName Table to delete
     */
    public void deleteAccumuloTable(String tableName)
    {
        try {
            connector.tableOperations().delete(tableName);
        }
        catch (AccumuloException | AccumuloSecurityException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to delete Accumulo table", e);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(ACCUMULO_TABLE_DNE, "Failed to delete Accumulo table, does not exist", e);
        }
    }

    /**
     * Renames the given Accumulo table to the new name
     *
     * @param oldName Old table name
     * @param newName New table name
     */
    public void renameAccumuloTable(String oldName, String newName)
    {
        try {
            connector.tableOperations().rename(oldName, newName);
        }
        catch (AccumuloSecurityException | AccumuloException e) {
            throw new PrestoException(UNEXPECTED_ACCUMULO_ERROR, "Failed to rename table", e);
        }
        catch (TableNotFoundException e) {
            throw new PrestoException(ACCUMULO_TABLE_DNE, "Failed to rename table, old table does not exist", e);
        }
        catch (TableExistsException e) {
            throw new PrestoException(ACCUMULO_TABLE_EXISTS, "Failed to rename table, new table already exists", e);
        }
    }
}
