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
package com.facebook.presto.hive.metastore;

import com.facebook.presto.hive.HiveCluster;
import com.facebook.presto.hive.HiveViewNotSupportedException;
import com.facebook.presto.hive.PartitionAlreadyExistsException;
import com.facebook.presto.hive.PartitionNotFoundException;
import com.facebook.presto.hive.RetryDriver;
import com.facebook.presto.hive.SchemaAlreadyExistsException;
import com.facebook.presto.hive.TableAlreadyExistsException;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.TableNotFoundException;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Iterables;
import org.apache.hadoop.hive.metastore.TableType;
import org.apache.hadoop.hive.metastore.api.AlreadyExistsException;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.HiveObjectPrivilege;
import org.apache.hadoop.hive.metastore.api.HiveObjectRef;
import org.apache.hadoop.hive.metastore.api.HiveObjectType;
import org.apache.hadoop.hive.metastore.api.InvalidObjectException;
import org.apache.hadoop.hive.metastore.api.InvalidOperationException;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.metastore.api.NoSuchObjectException;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrincipalType;
import org.apache.hadoop.hive.metastore.api.PrivilegeBag;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.Role;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.metastore.api.UnknownDBException;
import org.apache.thrift.TException;
import org.weakref.jmx.Flatten;
import org.weakref.jmx.Managed;

import javax.annotation.concurrent.ThreadSafe;
import javax.inject.Inject;

import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;
import java.util.concurrent.Callable;
import java.util.function.Function;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_METASTORE_ERROR;
import static com.facebook.presto.hive.HiveUtil.PRESTO_VIEW_FLAG;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.parsePrivilege;
import static com.facebook.presto.hive.metastore.MetastoreUtil.toGrants;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Iterables.getOnlyElement;
import static com.google.common.collect.Sets.newHashSet;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
import static java.util.stream.Collectors.toSet;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
import static org.apache.hadoop.hive.metastore.api.PrincipalType.USER;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.HIVE_FILTER_FIELD_PARAMS;

@ThreadSafe
public class ThriftHiveMetastore
        implements HiveMetastore
{
    private final ThriftHiveMetastoreStats stats = new ThriftHiveMetastoreStats();
    protected final HiveCluster clientProvider;

    @Inject
    public ThriftHiveMetastore(HiveCluster hiveCluster)
    {
        this.clientProvider = requireNonNull(hiveCluster, "hiveCluster is null");
    }

    @Managed
    @Flatten
    public ThriftHiveMetastoreStats getStats()
    {
        return stats;
    }

    @Override
    @Managed
    public void flushCache()
    {
    }

    @Override
    public List<String> getAllDatabases()
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getAllDatabases", stats.getGetAllDatabases().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return client.getAllDatabases();
                        }
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Optional<Database> getDatabase(String databaseName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getDatabase", stats.getGetDatabase().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return Optional.of(client.getDatabase(databaseName));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Optional<List<String>> getAllTables(String databaseName)
    {
        Callable<List<String>> getAllTables = stats.getGetAllTables().wrap(() -> {
            try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                return client.getAllTables(databaseName);
            }
        });

        Callable<Void> getDatabase = stats.getGetDatabase().wrap(() -> {
            try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                client.getDatabase(databaseName);
                return null;
            }
        });

        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllTables", () -> {
                        List<String> tables = getAllTables.call();
                        if (tables.isEmpty()) {
                            // Check to see if the database exists
                            getDatabase.call();
                        }
                        return Optional.of(tables);
                    });
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Optional<Table> getTable(String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class, HiveViewNotSupportedException.class)
                    .stopOnIllegalExceptions()
                    .run("getTable", stats.getGetTable().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            org.apache.hadoop.hive.metastore.api.Table table = client.getTable(databaseName, tableName);
                            if (table.getTableType().equals(TableType.VIRTUAL_VIEW.name()) && (!isPrestoView(table))) {
                                throw new HiveViewNotSupportedException(new SchemaTableName(databaseName, tableName));
                            }
                            return Optional.of(table);
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Optional<List<String>> getAllViews(String databaseName)
    {
        try {
            return retry()
                    .stopOn(UnknownDBException.class)
                    .stopOnIllegalExceptions()
                    .run("getAllViews", stats.getGetAllViews().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            String filter = HIVE_FILTER_FIELD_PARAMS + PRESTO_VIEW_FLAG + " = \"true\"";
                            return Optional.of(client.getTableNamesByFilter(databaseName, filter));
                        }
                    }));
        }
        catch (UnknownDBException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void createDatabase(Database database)
    {
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("createDatabase", stats.getCreateDatabase().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.createDatabase(database);
                        }
                        return null;
                    }));
        }
        catch (AlreadyExistsException e) {
            throw new SchemaAlreadyExistsException(database.getName());
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void dropDatabase(String databaseName)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, InvalidOperationException.class)
                    .stopOnIllegalExceptions()
                    .run("dropDatabase", stats.getDropDatabase().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.dropDatabase(databaseName, false, false);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void alterDatabase(String databaseName, Database database)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterDatabase", stats.getAlterDatabase().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.alterDatabase(databaseName, database);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(databaseName);
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void createTable(Table table)
    {
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("createTable", stats.getCreateTable().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.createTable(table);
                        }
                        return null;
                    }));
        }
        catch (AlreadyExistsException e) {
            throw new TableAlreadyExistsException(new SchemaTableName(table.getDbName(), table.getTableName()));
        }
        catch (NoSuchObjectException e) {
            throw new SchemaNotFoundException(table.getDbName());
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void dropTable(String databaseName, String tableName, boolean deleteData)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("dropTable", stats.getDropTable().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.dropTable(databaseName, tableName, deleteData);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void alterTable(String databaseName, String tableName, org.apache.hadoop.hive.metastore.api.Table table)
    {
        try {
            retry()
                    .stopOn(InvalidOperationException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterTable", stats.getAlterTable().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            Optional<Table> source = getTable(databaseName, tableName);
                            if (!source.isPresent()) {
                                throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
                            }
                            client.alterTable(databaseName, tableName, table);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (InvalidOperationException | MetaException e) {
            throw Throwables.propagate(e);
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private boolean isPrestoView(org.apache.hadoop.hive.metastore.api.Table table)
    {
        return "true".equals(table.getParameters().get(PRESTO_VIEW_FLAG));
    }

    @Override
    public Optional<List<String>> getPartitionNames(String databaseName, String tableName)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNames", stats.getGetPartitionNames().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return Optional.of(client.getPartitionNames(databaseName, tableName));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Optional<List<String>> getPartitionNamesByParts(String databaseName, String tableName, List<String> parts)
    {
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionNamesByParts", stats.getGetPartitionNamesPs().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return Optional.of(client.getPartitionNamesFiltered(databaseName, tableName, parts));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void addPartitions(String databaseName, String tableName, List<Partition> partitions)
    {
        if (partitions.isEmpty()) {
            return;
        }
        try {
            retry()
                    .stopOn(AlreadyExistsException.class, InvalidObjectException.class, MetaException.class, NoSuchObjectException.class, PrestoException.class)
                    .stopOnIllegalExceptions()
                    .run("addPartitions", stats.getAddPartitions().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            int partitionsAdded = client.addPartitions(partitions);
                            if (partitionsAdded != partitions.size()) {
                                throw new PrestoException(HIVE_METASTORE_ERROR,
                                        format("Hive metastore only added %s of %s partitions", partitionsAdded, partitions.size()));
                            }
                            return null;
                        }
                    }));
        }
        catch (AlreadyExistsException e) {
            throw new PartitionAlreadyExistsException(new SchemaTableName(databaseName, tableName), Optional.empty());
        }
        catch (NoSuchObjectException e) {
            throw new TableNotFoundException(new SchemaTableName(databaseName, tableName));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void dropPartition(String databaseName, String tableName, List<String> parts, boolean deleteData)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("dropPartition", stats.getDropPartition().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.dropPartition(databaseName, tableName, parts, deleteData);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), parts);
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void alterPartition(String databaseName, String tableName, Partition partition)
    {
        try {
            retry()
                    .stopOn(NoSuchObjectException.class, MetaException.class)
                    .stopOnIllegalExceptions()
                    .run("alterPartition", stats.getAlterPartition().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            client.alterPartition(databaseName, tableName, partition);
                        }
                        return null;
                    }));
        }
        catch (NoSuchObjectException e) {
            throw new PartitionNotFoundException(new SchemaTableName(databaseName, tableName), partition.getValues());
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Optional<Partition> getPartition(String databaseName, String tableName, List<String> partitionValues)
    {
        requireNonNull(partitionValues, "partitionValues is null");
        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartition", stats.getGetPartition().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return Optional.of(client.getPartition(databaseName, tableName, partitionValues));
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            return Optional.empty();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public List<Partition> getPartitionsByNames(String databaseName, String tableName, List<String> partitionNames)
    {
        requireNonNull(partitionNames, "partitionNames is null");
        checkArgument(!Iterables.isEmpty(partitionNames), "partitionNames is empty");

        try {
            return retry()
                    .stopOn(NoSuchObjectException.class)
                    .stopOnIllegalExceptions()
                    .run("getPartitionsByNames", stats.getGetPartitionsByNames().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            return client.getPartitionsByNames(databaseName, tableName, partitionNames);
                        }
                    }));
        }
        catch (NoSuchObjectException e) {
            // assume none of the partitions in the batch are available
            return ImmutableList.of();
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Set<String> getRoles(String user)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("listRoles", stats.getLoadRoles().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            List<Role> roles = client.listRoles(user, USER);
                            if (roles == null) {
                                return ImmutableSet.<String>of();
                            }
                            return ImmutableSet.copyOf(roles.stream()
                                    .map(Role::getRoleName)
                                    .collect(toSet()));
                        }
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public Set<HivePrivilegeInfo> getDatabasePrivileges(String user, String databaseName)
    {
        ImmutableSet.Builder<HivePrivilegeInfo> privileges = ImmutableSet.builder();

        if (isDatabaseOwner(user, databaseName)) {
            privileges.add(new HivePrivilegeInfo(OWNERSHIP, true));
        }
        privileges.addAll(getPrivileges(user, new HiveObjectRef(HiveObjectType.DATABASE, databaseName, null, null, null)));

        return privileges.build();
    }

    @Override
    public Set<HivePrivilegeInfo> getTablePrivileges(String user, String databaseName, String tableName)
    {
        ImmutableSet.Builder<HivePrivilegeInfo> privileges = ImmutableSet.builder();

        if (isTableOwner(user, databaseName, tableName)) {
            privileges.add(new HivePrivilegeInfo(OWNERSHIP, true));
        }
        privileges.addAll(getPrivileges(user, new HiveObjectRef(HiveObjectType.TABLE, databaseName, tableName, null, null)));

        return privileges.build();
    }

    @Override
    public void grantTablePrivileges(String databaseName, String tableName, String grantee, Set<PrivilegeGrantInfo> requestedPrivileges)
    {
        checkArgument(!containsAllPrivilege(requestedPrivileges), "\"ALL\" not supported in PrivilegeGrantInfo.privilege");

        try {
            Set<HivePrivilegeInfo> existingPrivileges = getTablePrivileges(grantee, databaseName, tableName);

            Set<PrivilegeGrantInfo> privilegesToGrant = newHashSet(requestedPrivileges);
            for (Iterator<PrivilegeGrantInfo> iterator = privilegesToGrant.iterator(); iterator.hasNext(); ) {
                HivePrivilegeInfo requestedPrivilege = getOnlyElement(parsePrivilege(iterator.next()));

                for (HivePrivilegeInfo existingPrivilege : existingPrivileges) {
                    if ((requestedPrivilege.isContainedIn(existingPrivilege))) {
                        iterator.remove();
                    }
                    else if (existingPrivilege.isContainedIn(requestedPrivilege)) {
                        throw new PrestoException(NOT_SUPPORTED, format(
                                "Granting %s WITH GRANT OPTION is not supported while %s possesses %s",
                                requestedPrivilege.getHivePrivilege().name(),
                                grantee,
                                requestedPrivilege.getHivePrivilege().name()));
                    }
              }
            }

            if (privilegesToGrant.isEmpty()) {
                return;
            }

            retry()
                    .stopOnIllegalExceptions()
                    .run("grantTablePrivileges", stats.getGrantTablePrivileges().wrap(() -> {
                        try (HiveMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                            PrincipalType principalType;

                            if (metastoreClient.getRoleNames().contains(grantee)) {
                                principalType = ROLE;
                            }
                            else {
                                principalType = USER;
                            }

                            ImmutableList.Builder<HiveObjectPrivilege> privilegeBagBuilder = ImmutableList.builder();
                            for (PrivilegeGrantInfo privilegeGrantInfo : privilegesToGrant) {
                                privilegeBagBuilder.add(
                                        new HiveObjectPrivilege(new HiveObjectRef(HiveObjectType.TABLE, databaseName, tableName, null, null),
                                                grantee,
                                                principalType,
                                                privilegeGrantInfo));
                            }
                            // TODO: Check whether the user/role exists in the hive metastore.
                            metastoreClient.grantPrivileges(new PrivilegeBag(privilegeBagBuilder.build()));
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public void revokeTablePrivileges(String databaseName, String tableName, String grantee, Set<PrivilegeGrantInfo> requestedPrivileges)
    {
        checkArgument(!containsAllPrivilege(requestedPrivileges), "\"ALL\" not supported in PrivilegeGrantInfo.privilege");

        try {
            Set<HivePrivilege> existingHivePrivileges = getTablePrivileges(grantee, databaseName, tableName).stream()
                    .map(HivePrivilegeInfo::getHivePrivilege)
                    .collect(toSet());

            Set<PrivilegeGrantInfo> privilegesToRevoke = requestedPrivileges.stream()
                    .filter(privilegeGrantInfo -> existingHivePrivileges.contains(getOnlyElement(parsePrivilege(privilegeGrantInfo)).getHivePrivilege()))
                    .collect(toSet());

            if (privilegesToRevoke.isEmpty()) {
                return;
            }

            retry()
                    .stopOnIllegalExceptions()
                    .run("revokeTablePrivileges", stats.getRevokeTablePrivileges().wrap(() -> {
                        try (HiveMetastoreClient metastoreClient = clientProvider.createMetastoreClient()) {
                            PrincipalType principalType;

                            if (metastoreClient.getRoleNames().contains(grantee)) {
                                principalType = ROLE;
                            }
                            else {
                                principalType = USER;
                            }

                            ImmutableList.Builder<HiveObjectPrivilege> privilegeBagBuilder = ImmutableList.builder();
                            for (PrivilegeGrantInfo privilegeGrantInfo : privilegesToRevoke) {
                                privilegeBagBuilder.add(
                                        new HiveObjectPrivilege(
                                                new HiveObjectRef(HiveObjectType.TABLE, databaseName, tableName, null, null),
                                                grantee,
                                                principalType,
                                                privilegeGrantInfo));
                            }
                            // TODO: Check whether the user/role exists in the hive metastore.
                            metastoreClient.revokePrivileges(new PrivilegeBag(privilegeBagBuilder.build()));
                        }
                        return null;
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            if (e instanceof InterruptedException) {
                Thread.currentThread().interrupt();
            }
            throw Throwables.propagate(e);
        }
    }

    private boolean containsAllPrivilege(Set<PrivilegeGrantInfo> requestedPrivileges)
    {
        return requestedPrivileges.stream()
                .anyMatch(privilege -> privilege.getPrivilege().equalsIgnoreCase("all"));
    }

    private Set<HivePrivilegeInfo> getPrivileges(String user, HiveObjectRef objectReference)
    {
        try {
            return retry()
                    .stopOnIllegalExceptions()
                    .run("getPrivilegeSet", stats.getGetPrivilegeSet().wrap(() -> {
                        try (HiveMetastoreClient client = clientProvider.createMetastoreClient()) {
                            ImmutableSet.Builder<HivePrivilegeInfo> privileges = ImmutableSet.builder();
                            PrincipalPrivilegeSet privilegeSet = client.getPrivilegeSet(objectReference, user, null);
                            if (privilegeSet != null) {
                                Map<String, List<PrivilegeGrantInfo>> userPrivileges = privilegeSet.getUserPrivileges();
                                if (userPrivileges != null) {
                                    privileges.addAll(toGrants(userPrivileges.get(user)));
                                }
                                Map<String, List<PrivilegeGrantInfo>> rolePrivilegesMap = privilegeSet.getRolePrivileges();
                                if (rolePrivilegesMap != null) {
                                    for (List<PrivilegeGrantInfo> rolePrivileges : rolePrivilegesMap.values()) {
                                        privileges.addAll(toGrants(rolePrivileges));
                                    }
                                }
                                // We do not add the group permissions as Hive does not seem to process these
                            }

                            return privileges.build();
                        }
                    }));
        }
        catch (TException e) {
            throw new PrestoException(HIVE_METASTORE_ERROR, e);
        }
        catch (Exception e) {
            throw propagate(e);
        }
    }

    private RetryDriver retry()
    {
        return RetryDriver.retry()
                .exceptionMapper(getExceptionMapper())
                .stopOn(PrestoException.class);
    }

    protected Function<Exception, Exception> getExceptionMapper()
    {
        return identity();
    }

    private RuntimeException propagate(Throwable throwable)
    {
        if (throwable instanceof InterruptedException) {
            Thread.currentThread().interrupt();
        }
        throw Throwables.propagate(throwable);
    }
}
