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
package com.facebook.presto.accumulo.metadata;

import com.facebook.presto.accumulo.AccumuloModule;
import com.facebook.presto.accumulo.conf.AccumuloConfig;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableMap;
import io.airlift.json.ObjectMapperProvider;
import org.apache.curator.framework.CuratorFramework;
import org.apache.curator.framework.CuratorFrameworkFactory;
import org.apache.curator.retry.RetryForever;
import org.apache.zookeeper.KeeperException;

import javax.activity.InvalidActivityException;
import javax.inject.Inject;

import java.io.IOException;
import java.util.HashSet;
import java.util.Locale;
import java.util.Set;
import java.util.stream.Collectors;

import static com.facebook.presto.accumulo.AccumuloErrorCode.ZOOKEEPER_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static org.apache.zookeeper.KeeperException.Code.NONODE;

public class ZooKeeperMetadataManager
{
    private static final String DEFAULT_SCHEMA = "default";

    private final CuratorFramework curator;
    private final ObjectMapper mapper;

    @Inject
    public ZooKeeperMetadataManager(AccumuloConfig config, TypeManager typeManager)
    {
        requireNonNull(typeManager, "typeManager is null");

        // Create JSON deserializer for the AccumuloTable
        ObjectMapperProvider objectMapperProvider = new ObjectMapperProvider();
        objectMapperProvider.setJsonDeserializers(ImmutableMap.of(Type.class, new AccumuloModule.TypeDeserializer(typeManager)));
        mapper = objectMapperProvider.get();

        String zkMetadataRoot = config.getZkMetadataRoot();
        String zookeepers = config.getZooKeepers();

        // Create the connection to ZooKeeper to check if the metadata root exists
        CuratorFramework checkRoot = CuratorFrameworkFactory.newClient(zookeepers, new RetryForever(1000));
        checkRoot.start();

        try {
            // If the metadata root does not exist, create it
            if (checkRoot.checkExists().forPath(zkMetadataRoot) == null) {
                checkRoot.create().forPath(zkMetadataRoot);
            }
        }
        catch (Exception e) {
            throw new PrestoException(ZOOKEEPER_ERROR, "ZK error checking metadata root", e);
        }
        checkRoot.close();

        // Create the curator client framework to use for metadata management, set at the ZK root
        curator = CuratorFrameworkFactory.newClient(zookeepers + zkMetadataRoot, new RetryForever(1000));
        curator.start();

        try {
            // Create default schema should it not exist
            if (curator.checkExists().forPath("/" + DEFAULT_SCHEMA) == null) {
                curator.create().forPath("/" + DEFAULT_SCHEMA);
            }
        }
        catch (Exception e) {
            throw new PrestoException(ZOOKEEPER_ERROR, "ZK error checking/creating default schema", e);
        }
    }

    public Set<String> getSchemaNames()
    {
        try {
            Set<String> schemas = new HashSet<>();
            schemas.addAll(curator.getChildren().forPath("/"));
            return schemas;
        }
        catch (Exception e) {
            throw new PrestoException(ZOOKEEPER_ERROR, "Error fetching schemas", e);
        }
    }

    public Set<String> getTableNames(String schema)
    {
        String schemaPath = getSchemaPath(schema);
        boolean exists;
        try {
            exists = curator.checkExists().forPath(schemaPath) != null;
        }
        catch (Exception e) {
            throw new PrestoException(ZOOKEEPER_ERROR, "Error checking if schema exists", e);
        }

        if (exists) {
            try {
                Set<String> tables = new HashSet<>();
                tables.addAll(
                        curator.getChildren().forPath(schemaPath)
                                .stream()
                                .filter(x -> isAccumuloTable(new SchemaTableName(schema, x)))
                                .collect(Collectors.toList()));
                return tables;
            }
            catch (Exception e) {
                throw new PrestoException(ZOOKEEPER_ERROR, "Error fetching schemas", e);
            }
        }
        else {
            throw new PrestoException(ZOOKEEPER_ERROR, "No metadata for schema" + schema);
        }
    }

    public AccumuloTable getTable(SchemaTableName stName)
    {
        try {
            if (curator.checkExists().forPath(getTablePath(stName)) != null) {
                return toAccumuloTable(curator.getData().forPath(getTablePath(stName)));
            }

            return null;
        }
        catch (Exception e) {
            // Capture race condition between checkExists and getData
            if (e instanceof KeeperException && ((KeeperException) e).code() == NONODE) {
                return null;
            }

            throw new PrestoException(ZOOKEEPER_ERROR, "Error fetching table", e);
        }
    }

    public Set<String> getViewNames(String schema)
    {
        String schemaPath = getSchemaPath(schema);
        boolean exists;
        try {
            exists = curator.checkExists().forPath(schemaPath) != null;
        }
        catch (Exception e) {
            throw new PrestoException(ZOOKEEPER_ERROR, "Error checking if schema exists", e);
        }

        if (exists) {
            try {
                Set<String> tables = new HashSet<>();
                tables.addAll(
                        curator.getChildren().forPath(schemaPath)
                                .stream()
                                .filter(x -> isAccumuloView(new SchemaTableName(schema, x)))
                                .collect(Collectors.toList()));
                return tables;
            }
            catch (Exception e) {
                throw new PrestoException(ZOOKEEPER_ERROR, "Error fetching schemas", e);
            }
        }
        else {
            throw new PrestoException(ZOOKEEPER_ERROR, "No metadata for schema " + schema);
        }
    }

    public AccumuloView getView(SchemaTableName stName)
    {
        try {
            String tablePath = getTablePath(stName);
            if (curator.checkExists().forPath(tablePath) != null) {
                return toAccumuloView(curator.getData().forPath(tablePath));
            }

            return null;
        }
        catch (Exception e) {
            // Capture race condition between checkExists and getData
            if (e instanceof KeeperException && ((KeeperException) e).code() == NONODE) {
                return null;
            }

            throw new PrestoException(ZOOKEEPER_ERROR, "Error fetching view", e);
        }
    }

    public void createTableMetadata(AccumuloTable table)
    {
        SchemaTableName tableName = table.getSchemaTableName();
        String tablePath = getTablePath(tableName);
        try {
            if (curator.checkExists().forPath(tablePath) != null) {
                throw new InvalidActivityException(format("Metadata for table %s already exists", tableName));
            }
        }
        catch (Exception e) {
            throw new PrestoException(ZOOKEEPER_ERROR, "ZK error when checking if table already exists", e);
        }

        try {
            curator.create().creatingParentsIfNeeded().forPath(tablePath, toJsonBytes(table));
        }
        catch (Exception e) {
            throw new PrestoException(ZOOKEEPER_ERROR, "Error creating table znode in ZooKeeper", e);
        }
    }

    public void deleteTableMetadata(SchemaTableName tableName)
    {
        try {
            curator.delete().deletingChildrenIfNeeded().forPath(getTablePath(tableName));
        }
        catch (Exception e) {
            throw new PrestoException(ZOOKEEPER_ERROR, "ZK error when deleting table metadata", e);
        }
    }

    public void createViewMetadata(AccumuloView view)
    {
        SchemaTableName tableName = view.getSchemaTableName();
        String viewPath = getTablePath(tableName);
        try {
            if (curator.checkExists().forPath(viewPath) != null) {
                throw new InvalidActivityException(format("Metadata for view %s already exists", tableName));
            }
        }
        catch (Exception e) {
            throw new PrestoException(ZOOKEEPER_ERROR, "ZK error when checking if view already exists", e);
        }

        try {
            curator.create().creatingParentsIfNeeded().forPath(viewPath, toJsonBytes(view));
        }
        catch (Exception e) {
            throw new PrestoException(ZOOKEEPER_ERROR, "Error creating view znode in ZooKeeper", e);
        }
    }

    public void deleteViewMetadata(SchemaTableName tableName)
    {
        try {
            curator.delete().deletingChildrenIfNeeded().forPath(getTablePath(tableName));
        }
        catch (Exception e) {
            throw new PrestoException(ZOOKEEPER_ERROR, "ZK error when deleting view metadata", e);
        }
    }

    private static String getSchemaPath(String schema)
    {
        return "/" + schema.toLowerCase(Locale.ENGLISH);
    }

    private static String getSchemaPath(SchemaTableName tableName)
    {
        return getSchemaPath(tableName.getSchemaName());
    }

    private static String getTablePath(SchemaTableName tableName)
    {
        return getSchemaPath(tableName) + '/' + tableName.getTableName().toLowerCase(Locale.ENGLISH);
    }

    private boolean isAccumuloTable(SchemaTableName tableName)
    {
        try {
            String path = getTablePath(tableName);
            return curator.checkExists().forPath(path) != null && isAccumuloTable(curator.getData().forPath(path));
        }
        catch (Exception e) {
            // Capture race condition between checkExists and getData
            if (e instanceof KeeperException && ((KeeperException) e).code() == NONODE) {
                return false;
            }

            throw new PrestoException(ZOOKEEPER_ERROR, "Error checking if path %s is an AccumuloTable object", e);
        }
    }

    private boolean isAccumuloView(SchemaTableName tableName)
    {
        try {
            String path = getTablePath(tableName);
            return curator.checkExists().forPath(path) != null && isAccumuloView(curator.getData().forPath(path));
        }
        catch (Exception e) {
            // Capture race condition between checkExists and getData
            if (e instanceof KeeperException && ((KeeperException) e).code() == NONODE) {
                return false;
            }

            throw new PrestoException(ZOOKEEPER_ERROR, "Error checking if path is an AccumuloView object", e);
        }
    }

    private boolean isAccumuloTable(byte[] data)
            throws IOException
    {
        // AccumuloTable does not contain a 'data' node
        return !mapper.reader().readTree(new String(data)).has("data");
    }

    private boolean isAccumuloView(byte[] data)
            throws IOException
    {
        // AccumuloView contains a 'data' node
        return mapper.reader().readTree(new String(data)).has("data");
    }

    private AccumuloTable toAccumuloTable(byte[] data)
            throws IOException
    {
        return mapper.readValue(new String(data), AccumuloTable.class);
    }

    private AccumuloView toAccumuloView(byte[] data)
            throws IOException
    {
        return mapper.readValue(new String(data), AccumuloView.class);
    }

    private byte[] toJsonBytes(Object obj)
            throws IOException
    {
        return mapper.writeValueAsBytes(obj);
    }
}
