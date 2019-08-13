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
package com.facebook.presto.hive.metastore.thrift;

import com.facebook.presto.hive.HiveBasicStatistics;
import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PartitionWithStatistics;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.SemiTransactionalHiveMetastore;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.security.ConnectorIdentity;
import com.facebook.presto.spi.security.PrestoPrincipal;
import com.facebook.presto.spi.security.PrincipalType;
import com.facebook.presto.spi.security.RoleGrant;
import com.facebook.presto.spi.security.SelectedRole;
import com.facebook.presto.spi.statistics.ColumnStatisticType;
import com.facebook.presto.spi.type.ArrayType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.MapType;
import com.facebook.presto.spi.type.RowType;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.AbstractIterator;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.google.common.collect.Streams;
import com.google.common.primitives.Longs;
import org.apache.hadoop.hive.metastore.api.BinaryColumnStatsData;
import org.apache.hadoop.hive.metastore.api.BooleanColumnStatsData;
import org.apache.hadoop.hive.metastore.api.ColumnStatisticsObj;
import org.apache.hadoop.hive.metastore.api.Date;
import org.apache.hadoop.hive.metastore.api.DateColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Decimal;
import org.apache.hadoop.hive.metastore.api.DecimalColumnStatsData;
import org.apache.hadoop.hive.metastore.api.DoubleColumnStatsData;
import org.apache.hadoop.hive.metastore.api.FieldSchema;
import org.apache.hadoop.hive.metastore.api.LongColumnStatsData;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.PrincipalPrivilegeSet;
import org.apache.hadoop.hive.metastore.api.PrivilegeGrantInfo;
import org.apache.hadoop.hive.metastore.api.RolePrincipalGrant;
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;
import org.apache.hadoop.hive.serde2.typeinfo.PrimitiveTypeInfo;
import org.apache.hadoop.hive.serde2.typeinfo.TypeInfo;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.nio.ByteBuffer;
import java.time.LocalDate;
import java.util.ArrayDeque;
import java.util.Arrays;
import java.util.Collection;
import java.util.EnumSet;
import java.util.HashSet;
import java.util.List;
import java.util.Locale;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Queue;
import java.util.Set;
import java.util.function.Function;
import java.util.function.Predicate;
import java.util.stream.Stream;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveMetadata.AVRO_SCHEMA_URL_KEY;
import static com.facebook.presto.hive.HiveStorageFormat.AVRO;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBinaryColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDateColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDecimalColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createStringColumnStatistics;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.DELETE;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.INSERT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.OWNERSHIP;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.SELECT;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.HivePrivilege.UPDATE;
import static com.facebook.presto.hive.metastore.PrestoTableType.EXTERNAL_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.PrestoTableType.OTHER;
import static com.facebook.presto.hive.metastore.PrestoTableType.VIRTUAL_VIEW;
import static com.facebook.presto.spi.security.PrincipalType.ROLE;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX_VALUE;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MAX_VALUE_SIZE_IN_BYTES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.MIN_VALUE;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_DISTINCT_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_NON_NULL_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.NUMBER_OF_TRUE_VALUES;
import static com.facebook.presto.spi.statistics.ColumnStatisticType.TOTAL_SIZE_IN_BYTES;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static com.google.common.collect.ImmutableSet.toImmutableSet;
import static java.lang.Math.round;
import static java.lang.String.format;
import static java.util.Locale.ENGLISH;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.binaryStats;
import static org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.booleanStats;
import static org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.dateStats;
import static org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.decimalStats;
import static org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.doubleStats;
import static org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.longStats;
import static org.apache.hadoop.hive.metastore.api.ColumnStatisticsData.stringStats;
import static org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector.Category.PRIMITIVE;

public final class ThriftMetastoreUtil
{
    private static final String PUBLIC_ROLE_NAME = "public";
    private static final String ADMIN_ROLE_NAME = "admin";
    private static final String NUM_FILES = "numFiles";
    private static final String NUM_ROWS = "numRows";
    private static final String RAW_DATA_SIZE = "rawDataSize";
    private static final String TOTAL_SIZE = "totalSize";
    private static final Set<String> STATS_PROPERTIES = ImmutableSet.of(NUM_FILES, NUM_ROWS, RAW_DATA_SIZE, TOTAL_SIZE);

    private ThriftMetastoreUtil() {}

    public static org.apache.hadoop.hive.metastore.api.Database toMetastoreApiDatabase(Database database)
    {
        org.apache.hadoop.hive.metastore.api.Database result = new org.apache.hadoop.hive.metastore.api.Database();
        result.setName(database.getDatabaseName());
        database.getLocation().ifPresent(result::setLocationUri);
        result.setOwnerName(database.getOwnerName());
        result.setOwnerType(toMetastoreApiPrincipalType(database.getOwnerType()));
        database.getComment().ifPresent(result::setDescription);
        result.setParameters(database.getParameters());
        return result;
    }

    public static org.apache.hadoop.hive.metastore.api.Table toMetastoreApiTable(Table table, PrincipalPrivileges privileges)
    {
        org.apache.hadoop.hive.metastore.api.Table result = new org.apache.hadoop.hive.metastore.api.Table();
        result.setDbName(table.getDatabaseName());
        result.setTableName(table.getTableName());
        result.setOwner(table.getOwner());
        checkArgument(EnumSet.of(MANAGED_TABLE, EXTERNAL_TABLE, VIRTUAL_VIEW).contains(table.getTableType()), "Invalid table type: %s", table.getTableType());
        result.setTableType(table.getTableType().name());
        result.setParameters(table.getParameters());
        result.setPartitionKeys(table.getPartitionColumns().stream().map(ThriftMetastoreUtil::toMetastoreApiFieldSchema).collect(toList()));
        result.setSd(makeStorageDescriptor(table.getTableName(), table.getDataColumns(), table.getStorage()));
        result.setPrivileges(toMetastoreApiPrincipalPrivilegeSet(privileges));
        result.setViewOriginalText(table.getViewOriginalText().orElse(null));
        result.setViewExpandedText(table.getViewExpandedText().orElse(null));
        return result;
    }

    private static PrincipalPrivilegeSet toMetastoreApiPrincipalPrivilegeSet(PrincipalPrivileges privileges)
    {
        ImmutableMap.Builder<String, List<PrivilegeGrantInfo>> userPrivileges = ImmutableMap.builder();
        for (Map.Entry<String, Collection<HivePrivilegeInfo>> entry : privileges.getUserPrivileges().asMap().entrySet()) {
            userPrivileges.put(entry.getKey(), entry.getValue().stream()
                    .map(ThriftMetastoreUtil::toMetastoreApiPrivilegeGrantInfo)
                    .collect(toList()));
        }

        ImmutableMap.Builder<String, List<PrivilegeGrantInfo>> rolePrivileges = ImmutableMap.builder();
        for (Map.Entry<String, Collection<HivePrivilegeInfo>> entry : privileges.getRolePrivileges().asMap().entrySet()) {
            rolePrivileges.put(entry.getKey(), entry.getValue().stream()
                    .map(ThriftMetastoreUtil::toMetastoreApiPrivilegeGrantInfo)
                    .collect(toList()));
        }

        return new PrincipalPrivilegeSet(userPrivileges.build(), ImmutableMap.of(), rolePrivileges.build());
    }

    public static PrivilegeGrantInfo toMetastoreApiPrivilegeGrantInfo(HivePrivilegeInfo privilegeInfo)
    {
        return new PrivilegeGrantInfo(
                privilegeInfo.getHivePrivilege().name().toLowerCase(Locale.ENGLISH),
                0,
                privilegeInfo.getGrantor().getName(),
                fromPrestoPrincipalType(privilegeInfo.getGrantor().getType()),
                privilegeInfo.isGrantOption());
    }

    public static org.apache.hadoop.hive.metastore.api.PrincipalType toMetastoreApiPrincipalType(PrincipalType principalType)
    {
        switch (principalType) {
            case USER:
                return org.apache.hadoop.hive.metastore.api.PrincipalType.USER;
            case ROLE:
                return org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
            default:
                throw new IllegalArgumentException("Unsupported principal type: " + principalType);
        }
    }

    public static Stream<RoleGrant> listApplicableRoles(PrestoPrincipal principal, Function<PrestoPrincipal, Set<RoleGrant>> listRoleGrants)
    {
        Queue<PrestoPrincipal> queue = new ArrayDeque<>();
        queue.add(principal);
        Queue<RoleGrant> output = new ArrayDeque<>();
        Set<RoleGrant> seenRoles = new HashSet<>();
        return Streams.stream(new AbstractIterator<RoleGrant>()
        {
            @Override
            protected RoleGrant computeNext()
            {
                if (!output.isEmpty()) {
                    return output.remove();
                }
                if (queue.isEmpty()) {
                    return endOfData();
                }

                while (!queue.isEmpty()) {
                    Set<RoleGrant> grants = listRoleGrants.apply(queue.remove());
                    if (!grants.isEmpty()) {
                        for (RoleGrant grant : grants) {
                            if (seenRoles.add(grant)) {
                                output.add(grant);
                                queue.add(new PrestoPrincipal(ROLE, grant.getRoleName()));
                            }
                        }
                        break;
                    }
                }
                if (output.isEmpty()) {
                    return endOfData();
                }
                return output.remove();
            }
        });
    }

    public static boolean isRoleApplicable(SemiTransactionalHiveMetastore metastore, PrestoPrincipal principal, String role)
    {
        if (principal.getType() == ROLE && principal.getName().equals(role)) {
            return true;
        }
        return listApplicableRoles(metastore, principal)
                .anyMatch(role::equals);
    }

    public static Stream<String> listApplicableRoles(SemiTransactionalHiveMetastore metastore, PrestoPrincipal principal)
    {
        return listApplicableRoles(principal, metastore::listRoleGrants)
                .map(RoleGrant::getRoleName);
    }

    public static Stream<PrestoPrincipal> listEnabledPrincipals(SemiTransactionalHiveMetastore metastore, ConnectorIdentity identity)
    {
        return Stream.concat(
                Stream.of(new PrestoPrincipal(USER, identity.getUser())),
                listEnabledRoles(identity, metastore::listRoleGrants)
                        .map(role -> new PrestoPrincipal(ROLE, role)));
    }

    public static Stream<HivePrivilegeInfo> listEnabledTablePrivileges(SemiTransactionalHiveMetastore metastore, String databaseName, String tableName, ConnectorIdentity identity)
    {
        return listTablePrivileges(metastore, databaseName, tableName, listEnabledPrincipals(metastore, identity));
    }

    public static Stream<HivePrivilegeInfo> listApplicableTablePrivileges(SemiTransactionalHiveMetastore metastore, String databaseName, String tableName, String user)
    {
        PrestoPrincipal userPrincipal = new PrestoPrincipal(USER, user);
        Stream<PrestoPrincipal> principals = Stream.concat(
                Stream.of(userPrincipal),
                listApplicableRoles(metastore, userPrincipal)
                .map(role -> new PrestoPrincipal(ROLE, role)));
        return listTablePrivileges(metastore, databaseName, tableName, principals);
    }

    private static Stream<HivePrivilegeInfo> listTablePrivileges(SemiTransactionalHiveMetastore metastore, String databaseName, String tableName, Stream<PrestoPrincipal> principals)
    {
        return principals.flatMap(principal -> metastore.listTablePrivileges(databaseName, tableName, principal).stream());
    }

    public static boolean isRoleEnabled(ConnectorIdentity identity, Function<PrestoPrincipal, Set<RoleGrant>> listRoleGrants, String role)
    {
        if (role.equals(PUBLIC_ROLE_NAME)) {
            return true;
        }

        if (identity.getRole().isPresent() && identity.getRole().get().getType() == SelectedRole.Type.NONE) {
            return false;
        }

        PrestoPrincipal principal;
        if (!identity.getRole().isPresent() || identity.getRole().get().getType() == SelectedRole.Type.ALL) {
            principal = new PrestoPrincipal(USER, identity.getUser());
        }
        else {
            principal = new PrestoPrincipal(ROLE, identity.getRole().get().getRole().get());
        }

        if (principal.getType() == ROLE && principal.getName().equals(role)) {
            return true;
        }

        if (role.equals(ADMIN_ROLE_NAME)) {
            // The admin role must be enabled explicitly, and so it should checked above
            return false;
        }

        // all the above code could be removed and method semantic would remain the same, however it would be more expensive for some negative cases (see above)
        return listEnabledRoles(identity, listRoleGrants)
                .anyMatch(role::equals);
    }

    public static Stream<String> listEnabledRoles(ConnectorIdentity identity, Function<PrestoPrincipal, Set<RoleGrant>> listRoleGrants)
    {
        Optional<SelectedRole> role = identity.getRole();
        if (role.isPresent() && role.get().getType() == SelectedRole.Type.NONE) {
            return Stream.of(PUBLIC_ROLE_NAME);
        }
        PrestoPrincipal principal;
        if (!role.isPresent() || role.get().getType() == SelectedRole.Type.ALL) {
            principal = new PrestoPrincipal(USER, identity.getUser());
        }
        else {
            principal = new PrestoPrincipal(ROLE, role.get().getRole().get());
        }

        Stream<String> roles = Stream.of(PUBLIC_ROLE_NAME);

        if (principal.getType() == ROLE) {
            roles = Stream.concat(roles, Stream.of(principal.getName()));
        }

        return Stream.concat(
                roles,
                listApplicableRoles(principal, listRoleGrants)
                        .map(RoleGrant::getRoleName)
                        // The admin role must be enabled explicitly. If it is, it was added above.
                        .filter(Predicate.isEqual(ADMIN_ROLE_NAME).negate()));
    }

    public static org.apache.hadoop.hive.metastore.api.Partition toMetastoreApiPartition(PartitionWithStatistics partitionWithStatistics)
    {
        org.apache.hadoop.hive.metastore.api.Partition partition = toMetastoreApiPartition(partitionWithStatistics.getPartition());
        partition.setParameters(updateStatisticsParameters(partition.getParameters(), partitionWithStatistics.getStatistics().getBasicStatistics()));
        return partition;
    }

    public static org.apache.hadoop.hive.metastore.api.Partition toMetastoreApiPartition(Partition partition)
    {
        org.apache.hadoop.hive.metastore.api.Partition result = new org.apache.hadoop.hive.metastore.api.Partition();
        result.setDbName(partition.getDatabaseName());
        result.setTableName(partition.getTableName());
        result.setValues(partition.getValues());
        result.setSd(makeStorageDescriptor(partition.getTableName(), partition.getColumns(), partition.getStorage()));
        result.setParameters(partition.getParameters());
        return result;
    }

    public static Database fromMetastoreApiDatabase(org.apache.hadoop.hive.metastore.api.Database database)
    {
        String ownerName = "PUBLIC";
        PrincipalType ownerType = ROLE;
        if (database.getOwnerName() != null) {
            ownerName = database.getOwnerName();
            ownerType = fromMetastoreApiPrincipalType(database.getOwnerType());
        }

        Map<String, String> parameters = database.getParameters();
        if (parameters == null) {
            parameters = ImmutableMap.of();
        }

        return Database.builder()
                .setDatabaseName(database.getName())
                .setLocation(Optional.ofNullable(database.getLocationUri()))
                .setOwnerName(ownerName)
                .setOwnerType(ownerType)
                .setComment(Optional.ofNullable(database.getDescription()))
                .setParameters(parameters)
                .build();
    }

    public static Table fromMetastoreApiTable(org.apache.hadoop.hive.metastore.api.Table table)
    {
        StorageDescriptor storageDescriptor = table.getSd();
        if (storageDescriptor == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table is missing storage descriptor");
        }
        return fromMetastoreApiTable(table, storageDescriptor.getCols());
    }

    public static Table fromMetastoreApiTable(org.apache.hadoop.hive.metastore.api.Table table, List<FieldSchema> schema)
    {
        StorageDescriptor storageDescriptor = table.getSd();
        if (storageDescriptor == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table is missing storage descriptor");
        }

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(table.getDbName())
                .setTableName(table.getTableName())
                .setOwner(nullToEmpty(table.getOwner()))
                .setTableType(PrestoTableType.optionalValueOf(table.getTableType()).orElse(OTHER))
                .setDataColumns(schema.stream()
                        .map(ThriftMetastoreUtil::fromMetastoreApiFieldSchema)
                        .collect(toList()))
                .setPartitionColumns(table.getPartitionKeys().stream()
                        .map(ThriftMetastoreUtil::fromMetastoreApiFieldSchema)
                        .collect(toList()))
                .setParameters(table.getParameters() == null ? ImmutableMap.of() : table.getParameters())
                .setViewOriginalText(Optional.ofNullable(emptyToNull(table.getViewOriginalText())))
                .setViewExpandedText(Optional.ofNullable(emptyToNull(table.getViewExpandedText())));

        fromMetastoreApiStorageDescriptor(storageDescriptor, tableBuilder.getStorageBuilder(), table.getTableName());

        return tableBuilder.build();
    }

    public static boolean isAvroTableWithSchemaSet(org.apache.hadoop.hive.metastore.api.Table table)
    {
        if (table.getParameters() == null) {
            return false;
        }
        StorageDescriptor storageDescriptor = table.getSd();
        if (storageDescriptor == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table does not contain a storage descriptor: " + table);
        }
        SerDeInfo serdeInfo = storageDescriptor.getSerdeInfo();
        if (serdeInfo == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table storage descriptor is missing SerDe info");
        }

        return serdeInfo.getSerializationLib() != null &&
                table.getParameters().get(AVRO_SCHEMA_URL_KEY) != null &&
                serdeInfo.getSerializationLib().equals(AVRO.getSerDe());
    }

    public static Partition fromMetastoreApiPartition(org.apache.hadoop.hive.metastore.api.Partition partition)
    {
        StorageDescriptor storageDescriptor = partition.getSd();
        if (storageDescriptor == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Partition does not contain a storage descriptor: " + partition);
        }

        Partition.Builder partitionBuilder = Partition.builder()
                .setDatabaseName(partition.getDbName())
                .setTableName(partition.getTableName())
                .setValues(partition.getValues())
                .setColumns(storageDescriptor.getCols().stream()
                        .map(ThriftMetastoreUtil::fromMetastoreApiFieldSchema)
                        .collect(toList()))
                .setParameters(partition.getParameters());

        fromMetastoreApiStorageDescriptor(storageDescriptor, partitionBuilder.getStorageBuilder(), format("%s.%s", partition.getTableName(), partition.getValues()));

        return partitionBuilder.build();
    }

    public static HiveColumnStatistics fromMetastoreApiColumnStatistics(ColumnStatisticsObj columnStatistics, OptionalLong rowCount)
    {
        if (columnStatistics.getStatsData().isSetLongStats()) {
            LongColumnStatsData longStatsData = columnStatistics.getStatsData().getLongStats();
            OptionalLong min = longStatsData.isSetLowValue() ? OptionalLong.of(longStatsData.getLowValue()) : OptionalLong.empty();
            OptionalLong max = longStatsData.isSetHighValue() ? OptionalLong.of(longStatsData.getHighValue()) : OptionalLong.empty();
            OptionalLong nullsCount = longStatsData.isSetNumNulls() ? fromMetastoreNullsCount(longStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = longStatsData.isSetNumDVs() ? OptionalLong.of(longStatsData.getNumDVs()) : OptionalLong.empty();
            return createIntegerColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.getStatsData().isSetDoubleStats()) {
            DoubleColumnStatsData doubleStatsData = columnStatistics.getStatsData().getDoubleStats();
            OptionalDouble min = doubleStatsData.isSetLowValue() ? OptionalDouble.of(doubleStatsData.getLowValue()) : OptionalDouble.empty();
            OptionalDouble max = doubleStatsData.isSetHighValue() ? OptionalDouble.of(doubleStatsData.getHighValue()) : OptionalDouble.empty();
            OptionalLong nullsCount = doubleStatsData.isSetNumNulls() ? fromMetastoreNullsCount(doubleStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = doubleStatsData.isSetNumDVs() ? OptionalLong.of(doubleStatsData.getNumDVs()) : OptionalLong.empty();
            return createDoubleColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.getStatsData().isSetDecimalStats()) {
            DecimalColumnStatsData decimalStatsData = columnStatistics.getStatsData().getDecimalStats();
            Optional<BigDecimal> min = decimalStatsData.isSetLowValue() ? fromMetastoreDecimal(decimalStatsData.getLowValue()) : Optional.empty();
            Optional<BigDecimal> max = decimalStatsData.isSetHighValue() ? fromMetastoreDecimal(decimalStatsData.getHighValue()) : Optional.empty();
            OptionalLong nullsCount = decimalStatsData.isSetNumNulls() ? fromMetastoreNullsCount(decimalStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = decimalStatsData.isSetNumDVs() ? OptionalLong.of(decimalStatsData.getNumDVs()) : OptionalLong.empty();
            return createDecimalColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.getStatsData().isSetDateStats()) {
            DateColumnStatsData dateStatsData = columnStatistics.getStatsData().getDateStats();
            Optional<LocalDate> min = dateStatsData.isSetLowValue() ? fromMetastoreDate(dateStatsData.getLowValue()) : Optional.empty();
            Optional<LocalDate> max = dateStatsData.isSetHighValue() ? fromMetastoreDate(dateStatsData.getHighValue()) : Optional.empty();
            OptionalLong nullsCount = dateStatsData.isSetNumNulls() ? fromMetastoreNullsCount(dateStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = dateStatsData.isSetNumDVs() ? OptionalLong.of(dateStatsData.getNumDVs()) : OptionalLong.empty();
            return createDateColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.getStatsData().isSetBooleanStats()) {
            BooleanColumnStatsData booleanStatsData = columnStatistics.getStatsData().getBooleanStats();
            return createBooleanColumnStatistics(
                    booleanStatsData.isSetNumTrues() ? OptionalLong.of(booleanStatsData.getNumTrues()) : OptionalLong.empty(),
                    booleanStatsData.isSetNumFalses() ? OptionalLong.of(booleanStatsData.getNumFalses()) : OptionalLong.empty(),
                    booleanStatsData.isSetNumNulls() ? fromMetastoreNullsCount(booleanStatsData.getNumNulls()) : OptionalLong.empty());
        }
        if (columnStatistics.getStatsData().isSetStringStats()) {
            StringColumnStatsData stringStatsData = columnStatistics.getStatsData().getStringStats();
            OptionalLong maxColumnLength = stringStatsData.isSetMaxColLen() ? OptionalLong.of(stringStatsData.getMaxColLen()) : OptionalLong.empty();
            OptionalDouble averageColumnLength = stringStatsData.isSetAvgColLen() ? OptionalDouble.of(stringStatsData.getAvgColLen()) : OptionalDouble.empty();
            OptionalLong nullsCount = stringStatsData.isSetNumNulls() ? fromMetastoreNullsCount(stringStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = stringStatsData.isSetNumDVs() ? OptionalLong.of(stringStatsData.getNumDVs()) : OptionalLong.empty();
            return createStringColumnStatistics(
                    maxColumnLength,
                    getTotalSizeInBytes(averageColumnLength, rowCount, nullsCount),
                    nullsCount,
                    fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.getStatsData().isSetBinaryStats()) {
            BinaryColumnStatsData binaryStatsData = columnStatistics.getStatsData().getBinaryStats();
            OptionalLong maxColumnLength = binaryStatsData.isSetMaxColLen() ? OptionalLong.of(binaryStatsData.getMaxColLen()) : OptionalLong.empty();
            OptionalDouble averageColumnLength = binaryStatsData.isSetAvgColLen() ? OptionalDouble.of(binaryStatsData.getAvgColLen()) : OptionalDouble.empty();
            OptionalLong nullsCount = binaryStatsData.isSetNumNulls() ? fromMetastoreNullsCount(binaryStatsData.getNumNulls()) : OptionalLong.empty();
            return createBinaryColumnStatistics(
                    maxColumnLength,
                    getTotalSizeInBytes(averageColumnLength, rowCount, nullsCount),
                    nullsCount);
        }
        else {
            throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics data: " + columnStatistics);
        }
    }

    public static Optional<LocalDate> fromMetastoreDate(Date date)
    {
        if (date == null) {
            return Optional.empty();
        }
        return Optional.of(LocalDate.ofEpochDay(date.getDaysSinceEpoch()));
    }

    /**
     * Impala `COMPUTE STATS` will write -1 as the null count.
     *
     * @see <a href="https://issues.apache.org/jira/browse/IMPALA-7497">IMPALA-7497</a>
     */
    public static OptionalLong fromMetastoreNullsCount(long nullsCount)
    {
        if (nullsCount == -1L) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(nullsCount);
    }

    public static Optional<BigDecimal> fromMetastoreDecimal(@Nullable Decimal decimal)
    {
        if (decimal == null) {
            return Optional.empty();
        }
        return Optional.of(new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()));
    }

    public static OptionalLong getTotalSizeInBytes(OptionalDouble averageColumnLength, OptionalLong rowCount, OptionalLong nullsCount)
    {
        if (averageColumnLength.isPresent() && rowCount.isPresent() && nullsCount.isPresent()) {
            long nonNullsCount = rowCount.getAsLong() - nullsCount.getAsLong();
            if (nonNullsCount < 0) {
                return OptionalLong.empty();
            }
            return OptionalLong.of(round(averageColumnLength.getAsDouble() * nonNullsCount));
        }
        return OptionalLong.empty();
    }

    /**
     * Hive calculates NDV considering null as a distinct value
     */
    private static OptionalLong fromMetastoreDistinctValuesCount(OptionalLong distinctValuesCount, OptionalLong nullsCount, OptionalLong rowCount)
    {
        if (distinctValuesCount.isPresent() && nullsCount.isPresent() && rowCount.isPresent()) {
            return OptionalLong.of(fromMetastoreDistinctValuesCount(distinctValuesCount.getAsLong(), nullsCount.getAsLong(), rowCount.getAsLong()));
        }
        return OptionalLong.empty();
    }

    private static long fromMetastoreDistinctValuesCount(long distinctValuesCount, long nullsCount, long rowCount)
    {
        long nonNullsCount = rowCount - nullsCount;
        if (nullsCount > 0 && distinctValuesCount > 0) {
            distinctValuesCount--;
        }

        // normalize distinctValuesCount in case there is a non null element
        if (nonNullsCount > 0 && distinctValuesCount == 0) {
            distinctValuesCount = 1;
        }

        // the metastore may store an estimate, so the value stored may be higher than the total number of rows
        if (distinctValuesCount > nonNullsCount) {
            return nonNullsCount;
        }
        return distinctValuesCount;
    }

    public static Set<RoleGrant> fromRolePrincipalGrants(Collection<RolePrincipalGrant> grants)
    {
        return ImmutableSet.copyOf(grants.stream().map(ThriftMetastoreUtil::fromRolePrincipalGrant).collect(toList()));
    }

    public static RoleGrant fromRolePrincipalGrant(RolePrincipalGrant grant)
    {
        return new RoleGrant(
                new PrestoPrincipal(fromMetastoreApiPrincipalType(grant.getPrincipalType()), grant.getPrincipalName()),
                grant.getRoleName(),
                grant.isGrantOption());
    }

    public static org.apache.hadoop.hive.metastore.api.PrincipalType fromPrestoPrincipalType(PrincipalType principalType)
    {
        switch (principalType) {
            case USER:
                return org.apache.hadoop.hive.metastore.api.PrincipalType.USER;
            case ROLE:
                return org.apache.hadoop.hive.metastore.api.PrincipalType.ROLE;
            default:
                throw new IllegalArgumentException("Unsupported principal type: " + principalType);
        }
    }

    public static PrincipalType fromMetastoreApiPrincipalType(org.apache.hadoop.hive.metastore.api.PrincipalType principalType)
    {
        requireNonNull(principalType, "principalType is null");
        switch (principalType) {
            case USER:
                return USER;
            case ROLE:
                return ROLE;
            default:
                throw new IllegalArgumentException("Unsupported principal type: " + principalType);
        }
    }

    public static FieldSchema toMetastoreApiFieldSchema(Column column)
    {
        return new FieldSchema(column.getName(), column.getType().getHiveTypeName().toString(), column.getComment().orElse(null));
    }

    public static Column fromMetastoreApiFieldSchema(FieldSchema fieldSchema)
    {
        return new Column(fieldSchema.getName(), HiveType.valueOf(fieldSchema.getType()), Optional.ofNullable(emptyToNull(fieldSchema.getComment())));
    }

    public static void fromMetastoreApiStorageDescriptor(StorageDescriptor storageDescriptor, Storage.Builder builder, String tablePartitionName)
    {
        SerDeInfo serdeInfo = storageDescriptor.getSerdeInfo();
        if (serdeInfo == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, "Table storage descriptor is missing SerDe info");
        }

        builder.setStorageFormat(StorageFormat.createNullable(serdeInfo.getSerializationLib(), storageDescriptor.getInputFormat(), storageDescriptor.getOutputFormat()))
                .setLocation(nullToEmpty(storageDescriptor.getLocation()))
                .setBucketProperty(HiveBucketProperty.fromStorageDescriptor(storageDescriptor, tablePartitionName))
                .setSkewed(storageDescriptor.isSetSkewedInfo() && storageDescriptor.getSkewedInfo().isSetSkewedColNames() && !storageDescriptor.getSkewedInfo().getSkewedColNames().isEmpty())
                .setSerdeParameters(serdeInfo.getParameters() == null ? ImmutableMap.of() : serdeInfo.getParameters());
    }

    private static StorageDescriptor makeStorageDescriptor(String tableName, List<Column> columns, Storage storage)
    {
        if (storage.isSkewed()) {
            throw new IllegalArgumentException("Writing to skewed table/partition is not supported");
        }
        SerDeInfo serdeInfo = new SerDeInfo();
        serdeInfo.setName(tableName);
        serdeInfo.setSerializationLib(storage.getStorageFormat().getSerDeNullable());
        serdeInfo.setParameters(storage.getSerdeParameters());

        StorageDescriptor sd = new StorageDescriptor();
        sd.setLocation(emptyToNull(storage.getLocation()));
        sd.setCols(columns.stream()
                .map(ThriftMetastoreUtil::toMetastoreApiFieldSchema)
                .collect(toList()));
        sd.setSerdeInfo(serdeInfo);
        sd.setInputFormat(storage.getStorageFormat().getInputFormatNullable());
        sd.setOutputFormat(storage.getStorageFormat().getOutputFormatNullable());
        sd.setParameters(ImmutableMap.of());

        Optional<HiveBucketProperty> bucketProperty = storage.getBucketProperty();
        if (bucketProperty.isPresent()) {
            sd.setNumBuckets(bucketProperty.get().getBucketCount());
            sd.setBucketCols(bucketProperty.get().getBucketedBy());
            if (!bucketProperty.get().getSortedBy().isEmpty()) {
                sd.setSortCols(bucketProperty.get().getSortedBy().stream()
                        .map(column -> new Order(column.getColumnName(), column.getOrder().getHiveOrder()))
                        .collect(toList()));
            }
        }

        return sd;
    }

    public static Set<HivePrivilegeInfo> parsePrivilege(PrivilegeGrantInfo userGrant, Optional<PrestoPrincipal> grantee)
    {
        boolean withGrantOption = userGrant.isGrantOption();
        String name = userGrant.getPrivilege().toUpperCase(ENGLISH);
        PrestoPrincipal grantor = new PrestoPrincipal(fromMetastoreApiPrincipalType(userGrant.getGrantorType()), userGrant.getGrantor());
        switch (name) {
            case "ALL":
                return Arrays.stream(HivePrivilegeInfo.HivePrivilege.values())
                        .map(hivePrivilege -> new HivePrivilegeInfo(hivePrivilege, withGrantOption, grantor, grantee.orElse(grantor)))
                        .collect(toImmutableSet());
            case "SELECT":
                return ImmutableSet.of(new HivePrivilegeInfo(SELECT, withGrantOption, grantor, grantee.orElse(grantor)));
            case "INSERT":
                return ImmutableSet.of(new HivePrivilegeInfo(INSERT, withGrantOption, grantor, grantee.orElse(grantor)));
            case "UPDATE":
                return ImmutableSet.of(new HivePrivilegeInfo(UPDATE, withGrantOption, grantor, grantee.orElse(grantor)));
            case "DELETE":
                return ImmutableSet.of(new HivePrivilegeInfo(DELETE, withGrantOption, grantor, grantee.orElse(grantor)));
            case "OWNERSHIP":
                return ImmutableSet.of(new HivePrivilegeInfo(OWNERSHIP, withGrantOption, grantor, grantee.orElse(grantor)));
            default:
                throw new IllegalArgumentException("Unsupported privilege name: " + name);
        }
    }

    public static HiveBasicStatistics getHiveBasicStatistics(Map<String, String> parameters)
    {
        OptionalLong numFiles = parse(parameters.get(NUM_FILES));
        OptionalLong numRows = parse(parameters.get(NUM_ROWS));
        OptionalLong inMemoryDataSizeInBytes = parse(parameters.get(RAW_DATA_SIZE));
        OptionalLong onDiskDataSizeInBytes = parse(parameters.get(TOTAL_SIZE));
        return new HiveBasicStatistics(numFiles, numRows, inMemoryDataSizeInBytes, onDiskDataSizeInBytes);
    }

    private static OptionalLong parse(@Nullable String parameterValue)
    {
        if (parameterValue == null) {
            return OptionalLong.empty();
        }
        Long longValue = Longs.tryParse(parameterValue);
        if (longValue == null || longValue < 0) {
            return OptionalLong.empty();
        }
        return OptionalLong.of(longValue);
    }

    public static Map<String, String> updateStatisticsParameters(Map<String, String> parameters, HiveBasicStatistics statistics)
    {
        ImmutableMap.Builder<String, String> result = ImmutableMap.builder();

        parameters.forEach((key, value) -> {
            if (!STATS_PROPERTIES.contains(key)) {
                result.put(key, value);
            }
        });

        statistics.getFileCount().ifPresent(count -> result.put(NUM_FILES, Long.toString(count)));
        statistics.getRowCount().ifPresent(count -> result.put(NUM_ROWS, Long.toString(count)));
        statistics.getInMemoryDataSizeInBytes().ifPresent(size -> result.put(RAW_DATA_SIZE, Long.toString(size)));
        statistics.getOnDiskDataSizeInBytes().ifPresent(size -> result.put(TOTAL_SIZE, Long.toString(size)));

        return result.build();
    }

    public static ColumnStatisticsObj createMetastoreColumnStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics, OptionalLong rowCount)
    {
        TypeInfo typeInfo = columnType.getTypeInfo();
        checkArgument(typeInfo.getCategory() == PRIMITIVE, "unsupported type: %s", columnType);
        switch (((PrimitiveTypeInfo) typeInfo).getPrimitiveCategory()) {
            case BOOLEAN:
                return createBooleanStatistics(columnName, columnType, statistics);
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return createLongStatistics(columnName, columnType, statistics);
            case FLOAT:
            case DOUBLE:
                return createDoubleStatistics(columnName, columnType, statistics);
            case STRING:
            case VARCHAR:
            case CHAR:
                return createStringStatistics(columnName, columnType, statistics, rowCount);
            case DATE:
                return createDateStatistics(columnName, columnType, statistics);
            case TIMESTAMP:
                return createLongStatistics(columnName, columnType, statistics);
            case BINARY:
                return createBinaryStatistics(columnName, columnType, statistics, rowCount);
            case DECIMAL:
                return createDecimalStatistics(columnName, columnType, statistics);
            default:
                throw new IllegalArgumentException(format("unsupported type: %s", columnType));
        }
    }

    private static ColumnStatisticsObj createBooleanStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics)
    {
        BooleanColumnStatsData data = new BooleanColumnStatsData();
        statistics.getNullsCount().ifPresent(data::setNumNulls);
        statistics.getBooleanStatistics().ifPresent(booleanStatistics -> {
            booleanStatistics.getFalseCount().ifPresent(data::setNumFalses);
            booleanStatistics.getTrueCount().ifPresent(data::setNumTrues);
        });
        return new ColumnStatisticsObj(columnName, columnType.toString(), booleanStats(data));
    }

    private static ColumnStatisticsObj createLongStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics)
    {
        LongColumnStatsData data = new LongColumnStatsData();
        statistics.getIntegerStatistics().ifPresent(integerStatistics -> {
            integerStatistics.getMin().ifPresent(data::setLowValue);
            integerStatistics.getMax().ifPresent(data::setHighValue);
        });
        statistics.getNullsCount().ifPresent(data::setNumNulls);
        toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::setNumDVs);
        return new ColumnStatisticsObj(columnName, columnType.toString(), longStats(data));
    }

    private static ColumnStatisticsObj createDoubleStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics)
    {
        DoubleColumnStatsData data = new DoubleColumnStatsData();
        statistics.getDoubleStatistics().ifPresent(doubleStatistics -> {
            doubleStatistics.getMin().ifPresent(data::setLowValue);
            doubleStatistics.getMax().ifPresent(data::setHighValue);
        });
        statistics.getNullsCount().ifPresent(data::setNumNulls);
        toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::setNumDVs);
        return new ColumnStatisticsObj(columnName, columnType.toString(), doubleStats(data));
    }

    private static ColumnStatisticsObj createStringStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics, OptionalLong rowCount)
    {
        StringColumnStatsData data = new StringColumnStatsData();
        statistics.getNullsCount().ifPresent(data::setNumNulls);
        toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::setNumDVs);
        data.setMaxColLen(statistics.getMaxValueSizeInBytes().orElse(0));
        data.setAvgColLen(getAverageColumnLength(statistics.getTotalSizeInBytes(), rowCount, statistics.getNullsCount()).orElse(0));
        return new ColumnStatisticsObj(columnName, columnType.toString(), stringStats(data));
    }

    private static ColumnStatisticsObj createDateStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics)
    {
        DateColumnStatsData data = new DateColumnStatsData();
        statistics.getDateStatistics().ifPresent(dateStatistics -> {
            dateStatistics.getMin().ifPresent(value -> data.setLowValue(toMetastoreDate(value)));
            dateStatistics.getMax().ifPresent(value -> data.setHighValue(toMetastoreDate(value)));
        });
        statistics.getNullsCount().ifPresent(data::setNumNulls);
        toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::setNumDVs);
        return new ColumnStatisticsObj(columnName, columnType.toString(), dateStats(data));
    }

    private static ColumnStatisticsObj createBinaryStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics, OptionalLong rowCount)
    {
        BinaryColumnStatsData data = new BinaryColumnStatsData();
        statistics.getNullsCount().ifPresent(data::setNumNulls);
        data.setMaxColLen(statistics.getMaxValueSizeInBytes().orElse(0));
        data.setAvgColLen(getAverageColumnLength(statistics.getTotalSizeInBytes(), rowCount, statistics.getNullsCount()).orElse(0));
        return new ColumnStatisticsObj(columnName, columnType.toString(), binaryStats(data));
    }

    private static ColumnStatisticsObj createDecimalStatistics(String columnName, HiveType columnType, HiveColumnStatistics statistics)
    {
        DecimalColumnStatsData data = new DecimalColumnStatsData();
        statistics.getDecimalStatistics().ifPresent(decimalStatistics -> {
            decimalStatistics.getMin().ifPresent(value -> data.setLowValue(toMetastoreDecimal(value)));
            decimalStatistics.getMax().ifPresent(value -> data.setHighValue(toMetastoreDecimal(value)));
        });
        statistics.getNullsCount().ifPresent(data::setNumNulls);
        toMetastoreDistinctValuesCount(statistics.getDistinctValuesCount(), statistics.getNullsCount()).ifPresent(data::setNumDVs);
        return new ColumnStatisticsObj(columnName, columnType.toString(), decimalStats(data));
    }

    public static Date toMetastoreDate(LocalDate date)
    {
        return new Date(date.toEpochDay());
    }

    public static Decimal toMetastoreDecimal(BigDecimal decimal)
    {
        return new Decimal(ByteBuffer.wrap(decimal.unscaledValue().toByteArray()), (short) decimal.scale());
    }

    private static OptionalLong toMetastoreDistinctValuesCount(OptionalLong distinctValuesCount, OptionalLong nullsCount)
    {
        // metastore counts null as a distinct value
        if (distinctValuesCount.isPresent() && nullsCount.isPresent()) {
            return OptionalLong.of(distinctValuesCount.getAsLong() + (nullsCount.getAsLong() > 0 ? 1 : 0));
        }
        return OptionalLong.empty();
    }

    private static OptionalDouble getAverageColumnLength(OptionalLong totalSizeInBytes, OptionalLong rowCount, OptionalLong nullsCount)
    {
        if (totalSizeInBytes.isPresent() && rowCount.isPresent() && nullsCount.isPresent()) {
            long nonNullsCount = rowCount.getAsLong() - nullsCount.getAsLong();
            if (nonNullsCount <= 0) {
                return OptionalDouble.empty();
            }
            return OptionalDouble.of(((double) totalSizeInBytes.getAsLong()) / nonNullsCount);
        }
        return OptionalDouble.empty();
    }

    public static Set<ColumnStatisticType> getSupportedColumnStatistics(Type type)
    {
        if (type.equals(BOOLEAN)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_TRUE_VALUES);
        }
        if (isNumericType(type) || type.equals(DATE) || type.equals(TIMESTAMP)) {
            // TODO #7122 support non-legacy TIMESTAMP
            return ImmutableSet.of(MIN_VALUE, MAX_VALUE, NUMBER_OF_DISTINCT_VALUES, NUMBER_OF_NON_NULL_VALUES);
        }
        if (isVarcharType(type) || isCharType(type)) {
            // TODO Collect MIN,MAX once it is used by the optimizer
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, NUMBER_OF_DISTINCT_VALUES, TOTAL_SIZE_IN_BYTES, MAX_VALUE_SIZE_IN_BYTES);
        }
        if (type.equals(VARBINARY)) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, TOTAL_SIZE_IN_BYTES, MAX_VALUE_SIZE_IN_BYTES);
        }
        if (type instanceof ArrayType || type instanceof RowType || type instanceof MapType) {
            return ImmutableSet.of(NUMBER_OF_NON_NULL_VALUES, TOTAL_SIZE_IN_BYTES);
        }
        // Throwing here to make sure this method is updated when a new type is added in Hive connector
        throw new IllegalArgumentException("Unsupported type: " + type);
    }

    private static boolean isNumericType(Type type)
    {
        return type.equals(BIGINT) || type.equals(INTEGER) || type.equals(SMALLINT) || type.equals(TINYINT) ||
                type.equals(DOUBLE) || type.equals(REAL) ||
                type instanceof DecimalType;
    }
}
