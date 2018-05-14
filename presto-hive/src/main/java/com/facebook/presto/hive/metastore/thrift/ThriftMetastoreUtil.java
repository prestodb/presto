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

import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.HivePrivilegeInfo;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PrincipalPrivileges;
import com.facebook.presto.hive.metastore.PrincipalType;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
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
import org.apache.hadoop.hive.metastore.api.SerDeInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.StringColumnStatsData;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.Collection;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.metastore.HivePrivilegeInfo.parsePrivilege;
import static com.google.common.base.Strings.emptyToNull;
import static com.google.common.base.Strings.nullToEmpty;
import static java.lang.String.format;
import static java.util.stream.Collectors.toList;

public final class ThriftMetastoreUtil
{
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
        result.setTableType(table.getTableType());
        result.setParameters(table.getParameters());
        result.setPartitionKeys(table.getPartitionColumns().stream().map(ThriftMetastoreUtil::toMetastoreApiFieldSchema).collect(toList()));
        result.setSd(makeStorageDescriptor(table.getTableName(), table.getDataColumns(), table.getStorage()));
        result.setPrivileges(toMetastoreApiPrincipalPrivilegeSet(table.getOwner(), privileges));
        result.setViewOriginalText(table.getViewOriginalText().orElse(null));
        result.setViewExpandedText(table.getViewExpandedText().orElse(null));
        return result;
    }

    public static PrincipalPrivilegeSet toMetastoreApiPrincipalPrivilegeSet(String grantee, PrincipalPrivileges privileges)
    {
        ImmutableMap.Builder<String, List<PrivilegeGrantInfo>> userPrivileges = ImmutableMap.builder();
        for (Map.Entry<String, Collection<HivePrivilegeInfo>> entry : privileges.getUserPrivileges().asMap().entrySet()) {
            userPrivileges.put(entry.getKey(), entry.getValue().stream()
                    .map(privilegeInfo -> toMetastoreApiPrivilegeGrantInfo(grantee, privilegeInfo))
                    .collect(toList()));
        }

        ImmutableMap.Builder<String, List<PrivilegeGrantInfo>> rolePrivileges = ImmutableMap.builder();
        for (Map.Entry<String, Collection<HivePrivilegeInfo>> entry : privileges.getRolePrivileges().asMap().entrySet()) {
            rolePrivileges.put(entry.getKey(), entry.getValue().stream()
                    .map(privilegeInfo -> toMetastoreApiPrivilegeGrantInfo(grantee, privilegeInfo))
                    .collect(toList()));
        }

        return new PrincipalPrivilegeSet(userPrivileges.build(), ImmutableMap.of(), rolePrivileges.build());
    }

    public static PrivilegeGrantInfo toMetastoreApiPrivilegeGrantInfo(String grantee, HivePrivilegeInfo privilegeInfo)
    {
        return new PrivilegeGrantInfo(
                privilegeInfo.getHivePrivilege().name().toLowerCase(),
                0,
                grantee,
                org.apache.hadoop.hive.metastore.api.PrincipalType.USER, privilegeInfo.isGrantOption());
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

    public static Set<HivePrivilegeInfo> toGrants(List<PrivilegeGrantInfo> userGrants)
    {
        if (userGrants == null) {
            return ImmutableSet.of();
        }

        ImmutableSet.Builder<HivePrivilegeInfo> privileges = ImmutableSet.builder();
        for (PrivilegeGrantInfo userGrant : userGrants) {
            privileges.addAll(parsePrivilege(userGrant));
        }
        return privileges.build();
    }

    public static Database fromMetastoreApiDatabase(org.apache.hadoop.hive.metastore.api.Database database)
    {
        String ownerName = "PUBLIC";
        PrincipalType ownerType = PrincipalType.ROLE;
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

        Table.Builder tableBuilder = Table.builder()
                .setDatabaseName(table.getDbName())
                .setTableName(table.getTableName())
                .setOwner(nullToEmpty(table.getOwner()))
                .setTableType(table.getTableType())
                .setDataColumns(storageDescriptor.getCols().stream()
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

    public static HiveColumnStatistics fromMetastoreApiColumnStatistics(ColumnStatisticsObj columnStatistics)
    {
        if (columnStatistics.getStatsData().isSetLongStats()) {
            LongColumnStatsData longStatsData = columnStatistics.getStatsData().getLongStats();
            return new HiveColumnStatistics<>(
                    longStatsData.isSetLowValue() ? Optional.of(longStatsData.getLowValue()) : Optional.empty(),
                    longStatsData.isSetHighValue() ? Optional.of(longStatsData.getHighValue()) : Optional.empty(),
                    OptionalLong.empty(),
                    OptionalDouble.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    longStatsData.isSetNumNulls() ? OptionalLong.of(longStatsData.getNumNulls()) : OptionalLong.empty(),
                    longStatsData.isSetNumDVs() ? OptionalLong.of(longStatsData.getNumDVs()) : OptionalLong.empty());
        }
        else if (columnStatistics.getStatsData().isSetDoubleStats()) {
            DoubleColumnStatsData doubleStatsData = columnStatistics.getStatsData().getDoubleStats();
            return new HiveColumnStatistics<>(
                    doubleStatsData.isSetLowValue() ? Optional.of(doubleStatsData.getLowValue()) : Optional.empty(),
                    doubleStatsData.isSetHighValue() ? Optional.of(doubleStatsData.getHighValue()) : Optional.empty(),
                    OptionalLong.empty(),
                    OptionalDouble.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    doubleStatsData.isSetNumNulls() ? OptionalLong.of(doubleStatsData.getNumNulls()) : OptionalLong.empty(),
                    doubleStatsData.isSetNumDVs() ? OptionalLong.of(doubleStatsData.getNumDVs()) : OptionalLong.empty());
        }
        else if (columnStatistics.getStatsData().isSetDecimalStats()) {
            DecimalColumnStatsData decimalStatsData = columnStatistics.getStatsData().getDecimalStats();
            return new HiveColumnStatistics<>(
                    decimalStatsData.isSetLowValue() ? fromMetastoreDecimal(decimalStatsData.getLowValue()) : Optional.empty(),
                    decimalStatsData.isSetHighValue() ? fromMetastoreDecimal(decimalStatsData.getHighValue()) : Optional.empty(),
                    OptionalLong.empty(),
                    OptionalDouble.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    decimalStatsData.isSetNumNulls() ? OptionalLong.of(decimalStatsData.getNumNulls()) : OptionalLong.empty(),
                    decimalStatsData.isSetNumDVs() ? OptionalLong.of(decimalStatsData.getNumDVs()) : OptionalLong.empty());
        }
        else if (columnStatistics.getStatsData().isSetBooleanStats()) {
            BooleanColumnStatsData booleanStatsData = columnStatistics.getStatsData().getBooleanStats();
            return new HiveColumnStatistics<>(
                    Optional.empty(),
                    Optional.empty(),
                    OptionalLong.empty(),
                    OptionalDouble.empty(),
                    booleanStatsData.isSetNumTrues() ? OptionalLong.of(booleanStatsData.getNumTrues()) : OptionalLong.empty(),
                    booleanStatsData.isSetNumFalses() ? OptionalLong.of(booleanStatsData.getNumFalses()) : OptionalLong.empty(),
                    booleanStatsData.isSetNumNulls() ? OptionalLong.of(booleanStatsData.getNumNulls()) : OptionalLong.empty(),
                    booleanStatsData.isSetNumFalses() && booleanStatsData.isSetNumTrues() ?
                            OptionalLong.of((booleanStatsData.getNumFalses() > 0 ? 1 : 0) + (booleanStatsData.getNumTrues() > 0 ? 1 : 0)) : OptionalLong.empty());
        }
        else if (columnStatistics.getStatsData().isSetDateStats()) {
            DateColumnStatsData dateStatsData = columnStatistics.getStatsData().getDateStats();
            return new HiveColumnStatistics<>(
                    dateStatsData.isSetLowValue() ? fromMetastoreDate(dateStatsData.getLowValue()) : Optional.empty(),
                    dateStatsData.isSetHighValue() ? fromMetastoreDate(dateStatsData.getHighValue()) : Optional.empty(),
                    OptionalLong.empty(),
                    OptionalDouble.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    dateStatsData.isSetNumNulls() ? OptionalLong.of(dateStatsData.getNumNulls()) : OptionalLong.empty(),
                    dateStatsData.isSetNumDVs() ? OptionalLong.of(dateStatsData.getNumDVs()) : OptionalLong.empty());
        }
        else if (columnStatistics.getStatsData().isSetStringStats()) {
            StringColumnStatsData stringStatsData = columnStatistics.getStatsData().getStringStats();
            return new HiveColumnStatistics<>(
                    Optional.empty(),
                    Optional.empty(),
                    stringStatsData.isSetMaxColLen() ? OptionalLong.of(stringStatsData.getMaxColLen()) : OptionalLong.empty(),
                    stringStatsData.isSetAvgColLen() ? OptionalDouble.of(stringStatsData.getAvgColLen()) : OptionalDouble.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    stringStatsData.isSetNumNulls() ? OptionalLong.of(stringStatsData.getNumNulls()) : OptionalLong.empty(),
                    stringStatsData.isSetNumDVs() ? OptionalLong.of(stringStatsData.getNumDVs()) : OptionalLong.empty());
        }
        else if (columnStatistics.getStatsData().isSetBinaryStats()) {
            BinaryColumnStatsData binaryStatsData = columnStatistics.getStatsData().getBinaryStats();
            return new HiveColumnStatistics<>(
                    Optional.empty(),
                    Optional.empty(),
                    binaryStatsData.isSetMaxColLen() ? OptionalLong.of(binaryStatsData.getMaxColLen()) : OptionalLong.empty(),
                    binaryStatsData.isSetAvgColLen() ? OptionalDouble.of(binaryStatsData.getAvgColLen()) : OptionalDouble.empty(),
                    OptionalLong.empty(),
                    OptionalLong.empty(),
                    binaryStatsData.isSetNumNulls() ? OptionalLong.of(binaryStatsData.getNumNulls()) : OptionalLong.empty(),
                    OptionalLong.empty());
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

    public static Optional<BigDecimal> fromMetastoreDecimal(@Nullable Decimal decimal)
    {
        if (decimal == null) {
            return Optional.empty();
        }
        return Optional.of(new BigDecimal(new BigInteger(decimal.getUnscaled()), decimal.getScale()));
    }

    public static PrincipalType fromMetastoreApiPrincipalType(org.apache.hadoop.hive.metastore.api.PrincipalType principalType)
    {
        switch (principalType) {
            case USER:
                return PrincipalType.USER;
            case ROLE:
                return PrincipalType.ROLE;
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
}
