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
package com.facebook.presto.hive.metastore.alluxio;

import alluxio.grpc.table.BinaryColumnStatsData;
import alluxio.grpc.table.BooleanColumnStatsData;
import alluxio.grpc.table.ColumnStatisticsData;
import alluxio.grpc.table.Date;
import alluxio.grpc.table.DateColumnStatsData;
import alluxio.grpc.table.Decimal;
import alluxio.grpc.table.DecimalColumnStatsData;
import alluxio.grpc.table.DoubleColumnStatsData;
import alluxio.grpc.table.FieldSchema;
import alluxio.grpc.table.Layout;
import alluxio.grpc.table.LongColumnStatsData;
import alluxio.grpc.table.StringColumnStatsData;
import alluxio.grpc.table.layout.hive.PartitionInfo;
import alluxio.shaded.client.com.google.protobuf.InvalidProtocolBufferException;
import com.facebook.presto.hive.HiveBucketProperty;
import com.facebook.presto.hive.HiveType;
import com.facebook.presto.hive.metastore.Column;
import com.facebook.presto.hive.metastore.Database;
import com.facebook.presto.hive.metastore.HiveColumnStatistics;
import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.PrestoTableType;
import com.facebook.presto.hive.metastore.SortingColumn;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.Lists;

import javax.annotation.Nullable;

import java.math.BigDecimal;
import java.math.BigInteger;
import java.time.LocalDate;
import java.util.List;
import java.util.Optional;
import java.util.OptionalDouble;
import java.util.OptionalLong;
import java.util.Set;

import static com.facebook.presto.hive.BucketFunctionType.HIVE_COMPATIBLE;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBinaryColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createBooleanColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDateColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDecimalColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createDoubleColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createIntegerColumnStatistics;
import static com.facebook.presto.hive.metastore.HiveColumnStatistics.createStringColumnStatistics;
import static com.facebook.presto.hive.metastore.MetastoreUtil.fromMetastoreDistinctValuesCount;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.fromMetastoreNullsCount;
import static com.facebook.presto.hive.metastore.thrift.ThriftMetastoreUtil.getTotalSizeInBytes;
import static com.facebook.presto.spi.security.PrincipalType.ROLE;
import static com.facebook.presto.spi.security.PrincipalType.USER;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static com.google.common.collect.ImmutableSet.toImmutableSet;

public class AlluxioProtoUtils
{
    private AlluxioProtoUtils() {}

    public static Database fromProto(alluxio.grpc.table.Database database)
    {
        return Database.builder()
                .setDatabaseName(database.getDbName())
                .setLocation(database.hasLocation() ? Optional.of(database.getLocation()) : Optional.empty())
                .setOwnerName(database.getOwnerName())
                .setOwnerType(database.getOwnerType() == alluxio.grpc.table.PrincipalType.USER ? USER : ROLE)
                .setComment(database.hasComment() ? Optional.of(database.getComment()) : Optional.empty())
                .setParameters(database.getParameterMap())
                .build();
    }

    public static Table fromProto(alluxio.grpc.table.TableInfo table)
    {
        if (!table.hasLayout()) {
            throw new UnsupportedOperationException("Unsupported table metadata. missing layout.");
        }
        Layout layout = table.getLayout();
        if (!alluxio.table.ProtoUtils.isHiveLayout(layout)) {
            throw new UnsupportedOperationException("Unsupported table layout: " + layout);
        }

        try {
            PartitionInfo partitionInfo = alluxio.table.ProtoUtils.toHiveLayout(layout);

            // compute the data columns
            Set<String> partitionColumns = table.getPartitionColsList().stream().map(FieldSchema::getName).collect(toImmutableSet());
            List<FieldSchema> dataColumns = table.getSchema().getColsList().stream().filter((field) -> !partitionColumns.contains(field.getName())).collect(toImmutableList());

            Table.Builder builder = Table.builder()
                    .setDatabaseName(table.getDbName())
                    .setTableName(table.getTableName())
                    .setOwner(table.getOwner())
                    // TODO - Alluxio should return the actual table type
                    .setTableType(PrestoTableType.OTHER)
                    .setDataColumns(dataColumns.stream().map(AlluxioProtoUtils::fromProto).collect(toImmutableList()))
                    .setPartitionColumns(table.getPartitionColsList().stream().map(AlluxioProtoUtils::fromProto).collect(toImmutableList()))
                    .setParameters(table.getParametersMap())
                    .setViewOriginalText(Optional.empty())
                    .setViewExpandedText(Optional.empty());
            alluxio.grpc.table.layout.hive.Storage storage = partitionInfo.getStorage();
            // TODO: We should also set storage parameters here when they are available in alluxio.grpc.table.layout.hive.Storage
            builder.getStorageBuilder()
                    .setSkewed(storage.getSkewed())
                    .setStorageFormat(fromProto(storage.getStorageFormat()))
                    .setLocation(storage.getLocation())
                    .setBucketProperty(storage.hasBucketProperty() ? fromProto(storage.getBucketProperty()) : Optional.empty())
                    .setSerdeParameters(storage.getStorageFormat().getSerdelibParametersMap());
            return builder.build();
        }
        catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to extract PartitionInfo from TableInfo", e);
        }
    }

    public static HiveColumnStatistics fromProto(ColumnStatisticsData columnStatistics, OptionalLong rowCount)
    {
        if (columnStatistics.hasLongStats()) {
            LongColumnStatsData longStatsData = columnStatistics.getLongStats();
            OptionalLong min = longStatsData.hasLowValue() ? OptionalLong.of(longStatsData.getLowValue()) : OptionalLong.empty();
            OptionalLong max = longStatsData.hasHighValue() ? OptionalLong.of(longStatsData.getHighValue()) : OptionalLong.empty();
            OptionalLong nullsCount = longStatsData.hasNumNulls() ? fromMetastoreNullsCount(longStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = longStatsData.hasNumDistincts() ? OptionalLong.of(longStatsData.getNumDistincts()) : OptionalLong.empty();
            return createIntegerColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.hasDoubleStats()) {
            DoubleColumnStatsData doubleStatsData = columnStatistics.getDoubleStats();
            OptionalDouble min = doubleStatsData.hasLowValue() ? OptionalDouble.of(doubleStatsData.getLowValue()) : OptionalDouble.empty();
            OptionalDouble max = doubleStatsData.hasHighValue() ? OptionalDouble.of(doubleStatsData.getHighValue()) : OptionalDouble.empty();
            OptionalLong nullsCount = doubleStatsData.hasNumNulls() ? fromMetastoreNullsCount(doubleStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = doubleStatsData.hasNumDistincts() ? OptionalLong.of(doubleStatsData.getNumDistincts()) : OptionalLong.empty();
            return createDoubleColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.hasDecimalStats()) {
            DecimalColumnStatsData decimalStatsData = columnStatistics.getDecimalStats();
            Optional<BigDecimal> min = decimalStatsData.hasLowValue() ? fromMetastoreDecimal(decimalStatsData.getLowValue()) : Optional.empty();
            Optional<BigDecimal> max = decimalStatsData.hasHighValue() ? fromMetastoreDecimal(decimalStatsData.getHighValue()) : Optional.empty();
            OptionalLong nullsCount = decimalStatsData.hasNumNulls() ? fromMetastoreNullsCount(decimalStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = decimalStatsData.hasNumDistincts() ? OptionalLong.of(decimalStatsData.getNumDistincts()) : OptionalLong.empty();
            return createDecimalColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.hasDateStats()) {
            DateColumnStatsData dateStatsData = columnStatistics.getDateStats();
            Optional<LocalDate> min = dateStatsData.hasLowValue() ? fromMetastoreDate(dateStatsData.getLowValue()) : Optional.empty();
            Optional<LocalDate> max = dateStatsData.hasHighValue() ? fromMetastoreDate(dateStatsData.getHighValue()) : Optional.empty();
            OptionalLong nullsCount = dateStatsData.hasNumNulls() ? fromMetastoreNullsCount(dateStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = dateStatsData.hasNumDistincts() ? OptionalLong.of(dateStatsData.getNumDistincts()) : OptionalLong.empty();
            return createDateColumnStatistics(min, max, nullsCount, fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.hasBooleanStats()) {
            BooleanColumnStatsData booleanStatsData = columnStatistics.getBooleanStats();
            OptionalLong trueCount = OptionalLong.empty();
            OptionalLong falseCount = OptionalLong.empty();
            if (booleanStatsData.hasNumTrues() && booleanStatsData.hasNumFalses() && (booleanStatsData.getNumFalses() != -1)) {
                trueCount = OptionalLong.of(booleanStatsData.getNumTrues());
                falseCount = OptionalLong.of(booleanStatsData.getNumFalses());
            }
            return createBooleanColumnStatistics(
                    trueCount,
                    falseCount,
                    booleanStatsData.hasNumNulls() ? fromMetastoreNullsCount(booleanStatsData.getNumNulls()) : OptionalLong.empty());
        }
        if (columnStatistics.hasStringStats()) {
            StringColumnStatsData stringStatsData = columnStatistics.getStringStats();
            OptionalLong maxColumnLength = stringStatsData.hasMaxColLen() ? OptionalLong.of(stringStatsData.getMaxColLen()) : OptionalLong.empty();
            OptionalDouble averageColumnLength = stringStatsData.hasAvgColLen() ? OptionalDouble.of(stringStatsData.getAvgColLen()) : OptionalDouble.empty();
            OptionalLong nullsCount = stringStatsData.hasNumNulls() ? fromMetastoreNullsCount(stringStatsData.getNumNulls()) : OptionalLong.empty();
            OptionalLong distinctValuesCount = stringStatsData.hasNumDistincts() ? OptionalLong.of(stringStatsData.getNumDistincts()) : OptionalLong.empty();
            return createStringColumnStatistics(
                    maxColumnLength,
                    getTotalSizeInBytes(averageColumnLength, rowCount, nullsCount),
                    nullsCount,
                    fromMetastoreDistinctValuesCount(distinctValuesCount, nullsCount, rowCount));
        }
        if (columnStatistics.hasBinaryStats()) {
            BinaryColumnStatsData binaryStatsData = columnStatistics.getBinaryStats();
            OptionalLong maxColumnLength = binaryStatsData.hasMaxColLen() ? OptionalLong.of(binaryStatsData.getMaxColLen()) : OptionalLong.empty();
            OptionalDouble averageColumnLength = binaryStatsData.hasAvgColLen() ? OptionalDouble.of(binaryStatsData.getAvgColLen()) : OptionalDouble.empty();
            OptionalLong nullsCount = binaryStatsData.hasNumNulls() ? fromMetastoreNullsCount(binaryStatsData.getNumNulls()) : OptionalLong.empty();
            return createBinaryColumnStatistics(
                    maxColumnLength,
                    getTotalSizeInBytes(averageColumnLength, rowCount, nullsCount),
                    nullsCount);
        }
        throw new PrestoException(HIVE_INVALID_METADATA, "Invalid column statistics data: " + columnStatistics);
    }

    private static Column fromProto(alluxio.grpc.table.FieldSchema column)
    {
        Optional<String> comment = column.hasComment() ? Optional.of(column.getComment()) : Optional.empty();
        return new Column(column.getName(), HiveType.valueOf(column.getType()), comment);
    }

    public static Partition fromProto(alluxio.grpc.table.layout.hive.PartitionInfo info)
    {
        Partition.Builder builder = Partition.builder()
                .setColumns(info.getDataColsList().stream().map(AlluxioProtoUtils::fromProto).collect(toImmutableList()))
                .setDatabaseName(info.getDbName())
                .setParameters(info.getParametersMap())
                .setValues(Lists.newArrayList(info.getValuesList()))
                .setTableName(info.getTableName());

        // TODO: We should also set storage parameters here when they are available in alluxio.grpc.table.layout.hive.Storage
        builder.getStorageBuilder()
                .setSkewed(info.getStorage().getSkewed())
                .setStorageFormat(fromProto(info.getStorage().getStorageFormat()))
                .setLocation(info.getStorage().getLocation())
                .setBucketProperty(info.getStorage().hasBucketProperty() ? fromProto(info.getStorage().getBucketProperty()) : Optional.empty())
                .setSerdeParameters(info.getStorage().getStorageFormat().getSerdelibParametersMap());

        return builder.build();
    }

    public static StorageFormat fromProto(alluxio.grpc.table.layout.hive.StorageFormat format)
    {
        return StorageFormat.create(format.getSerde(), format.getInputFormat(), format.getOutputFormat());
    }

    public static alluxio.grpc.table.layout.hive.PartitionInfo toPartitionInfo(alluxio.grpc.table.Partition part)
    {
        try {
            return alluxio.table.ProtoUtils.extractHiveLayout(part);
        }
        catch (InvalidProtocolBufferException e) {
            throw new IllegalArgumentException("Failed to extract PartitionInfo", e);
        }
    }

    public static List<alluxio.grpc.table.layout.hive.PartitionInfo> toPartitionInfoList(List<alluxio.grpc.table.Partition> parts)
    {
        return parts.stream().map(AlluxioProtoUtils::toPartitionInfo).collect(toImmutableList());
    }

    private static SortingColumn fromProto(alluxio.grpc.table.layout.hive.SortingColumn column)
    {
        if (column.getOrder().equals(alluxio.grpc.table.layout.hive.SortingColumn.SortingOrder.ASCENDING)) {
            return new SortingColumn(column.getColumnName(), SortingColumn.Order.ASCENDING);
        }
        if (column.getOrder().equals(alluxio.grpc.table.layout.hive.SortingColumn.SortingOrder.DESCENDING)) {
            return new SortingColumn(column.getColumnName(), SortingColumn.Order.DESCENDING);
        }
        throw new IllegalArgumentException("Invalid sort order: " + column.getOrder());
    }

    private static Optional<HiveBucketProperty> fromProto(alluxio.grpc.table.layout.hive.HiveBucketProperty property)
    {
        // must return empty if buckets <= 0
        if (!property.hasBucketCount() || property.getBucketCount() <= 0) {
            return Optional.empty();
        }
        List<SortingColumn> sortedBy = property.getSortedByList().stream().map(AlluxioProtoUtils::fromProto).collect(toImmutableList());
        return Optional.of(new HiveBucketProperty(property.getBucketedByList(), (int) property.getBucketCount(), sortedBy, HIVE_COMPATIBLE, Optional.empty()));
    }

    private static Optional<BigDecimal> fromMetastoreDecimal(@Nullable Decimal decimal)
    {
        if (decimal == null) {
            return Optional.empty();
        }
        return Optional.of(new BigDecimal(new BigInteger(decimal.getUnscaled().toByteArray()), decimal.getScale()));
    }

    private static Optional<LocalDate> fromMetastoreDate(@Nullable Date date)
    {
        if (date == null) {
            return Optional.empty();
        }
        return Optional.of(LocalDate.ofEpochDay(date.getDaysSinceEpoch()));
    }
}
