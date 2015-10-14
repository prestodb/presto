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
package com.facebook.presto.hive;

import com.facebook.presto.hive.metastore.HiveMetastore;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.SchemaNotFoundException;
import com.facebook.presto.spi.SchemaTableName;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.StandardSystemProperty;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.conf.HiveConf;
import org.apache.hadoop.hive.metastore.ProtectMode;
import org.apache.hadoop.hive.metastore.api.Database;
import org.apache.hadoop.hive.metastore.api.Order;
import org.apache.hadoop.hive.metastore.api.Partition;
import org.apache.hadoop.hive.metastore.api.SkewedInfo;
import org.apache.hadoop.hive.metastore.api.StorageDescriptor;
import org.apache.hadoop.hive.metastore.api.Table;
import org.apache.hadoop.hive.ql.exec.FileSinkOperator.RecordWriter;
import org.apache.hadoop.hive.ql.io.HiveOutputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspectorFactory;
import org.apache.hadoop.io.Text;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_DATABASE_LOCATION_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_METADATA;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_PATH_ALREADY_EXISTS;
import static com.facebook.presto.hive.HiveSplitManager.PRESTO_OFFLINE;
import static com.facebook.presto.hive.HiveUtil.checkCondition;
import static com.facebook.presto.hive.HiveUtil.isArrayType;
import static com.facebook.presto.hive.HiveUtil.isMapType;
import static com.facebook.presto.hive.HiveUtil.isRowType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Strings.isNullOrEmpty;
import static java.lang.String.format;
import static java.util.UUID.randomUUID;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.COMPRESSRESULT;
import static org.apache.hadoop.hive.metastore.MetaStoreUtils.getProtectMode;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaBooleanObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaByteArrayObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDateObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaDoubleObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaLongObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaStringObjectInspector;
import static org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorFactory.javaTimestampObjectInspector;

public final class HiveWriteUtils
{
    @SuppressWarnings("OctalInteger")
    private static final FsPermission ALL_PERMISSIONS = new FsPermission((short) 0777);

    private HiveWriteUtils()
    {
    }

    public static RecordWriter createRecordWriter(Path target, JobConf conf, Properties properties, String outputFormatName)
    {
        try {
            Object writer = Class.forName(outputFormatName).getConstructor().newInstance();
            boolean isCompressed = HiveConf.getBoolVar(conf, COMPRESSRESULT);
            return ((HiveOutputFormat<?, ?>) writer).getHiveRecordWriter(conf, target, Text.class, isCompressed, properties, Reporter.NULL);
        }
        catch (IOException | ReflectiveOperationException e) {
            throw Throwables.propagate(e);
        }
    }

    public static List<ObjectInspector> getJavaObjectInspectors(Iterable<Type> types)
    {
        ImmutableList.Builder<ObjectInspector> list = ImmutableList.builder();
        for (Type type : types) {
            list.add(getJavaObjectInspector(type));
        }
        return list.build();
    }

    private static ObjectInspector getJavaObjectInspector(Type type)
    {
        if (type.equals(BooleanType.BOOLEAN)) {
            return javaBooleanObjectInspector;
        }
        else if (type.equals(BigintType.BIGINT)) {
            return javaLongObjectInspector;
        }
        else if (type.equals(DoubleType.DOUBLE)) {
            return javaDoubleObjectInspector;
        }
        else if (type.equals(VarcharType.VARCHAR)) {
            return javaStringObjectInspector;
        }
        else if (type.equals(VarbinaryType.VARBINARY)) {
            return javaByteArrayObjectInspector;
        }
        else if (type.equals(DateType.DATE)) {
            return javaDateObjectInspector;
        }
        else if (type.equals(TimestampType.TIMESTAMP)) {
            return javaTimestampObjectInspector;
        }
        else if (isArrayType(type)) {
            return ObjectInspectorFactory.getStandardListObjectInspector(getJavaObjectInspector(type.getTypeParameters().get(0)));
        }
        else if (isMapType(type)) {
            ObjectInspector keyObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(0));
            ObjectInspector valueObjectInspector = getJavaObjectInspector(type.getTypeParameters().get(1));
            return ObjectInspectorFactory.getStandardMapObjectInspector(keyObjectInspector, valueObjectInspector);
        }
        else if (isRowType(type)) {
            return ObjectInspectorFactory.getStandardStructObjectInspector(
                    type.getTypeSignature().getLiteralParameters().stream()
                            .map(String.class::cast)
                            .collect(toList()),
                    type.getTypeParameters().stream()
                            .map(HiveWriteUtils::getJavaObjectInspector)
                            .collect(toList()));
        }
        throw new IllegalArgumentException("unsupported type: " + type);
    }

    public static Object getField(Type type, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        if (BooleanType.BOOLEAN.equals(type)) {
            return type.getBoolean(block, position);
        }
        if (BigintType.BIGINT.equals(type)) {
            return type.getLong(block, position);
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return type.getDouble(block, position);
        }
        if (VarcharType.VARCHAR.equals(type)) {
            return type.getSlice(block, position).toStringUtf8();
        }
        if (VarbinaryType.VARBINARY.equals(type)) {
            return type.getSlice(block, position).getBytes();
        }
        if (DateType.DATE.equals(type)) {
            // todo should this be adjusted to midnight in JVM timezone?
            long days = type.getLong(block, position);
            return new Date(TimeUnit.DAYS.toMillis(days));
        }
        if (TimestampType.TIMESTAMP.equals(type)) {
            long millisUtc = type.getLong(block, position);
            return new Timestamp(millisUtc);
        }
        if (isArrayType(type)) {
            Type elementType = type.getTypeParameters().get(0);

            Block arrayBlock = block.getObject(position, Block.class);

            List<Object> list = new ArrayList<>(arrayBlock.getPositionCount());
            for (int i = 0; i < arrayBlock.getPositionCount(); i++) {
                Object element = getField(elementType, arrayBlock, i);
                list.add(element);
            }

            return Collections.unmodifiableList(list);
        }
        if (isMapType(type)) {
            Type keyType = type.getTypeParameters().get(0);
            Type valueType = type.getTypeParameters().get(1);

            Block mapBlock = block.getObject(position, Block.class);
            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                Object key = getField(keyType, mapBlock, i);
                Object value = getField(valueType, mapBlock, i + 1);
                map.put(key, value);
            }

            return Collections.unmodifiableMap(map);
        }
        if (isRowType(type)) {
            Block rowBlock = block.getObject(position, Block.class);

            List<Type> fieldTypes = type.getTypeParameters();
            checkCondition(fieldTypes.size() == rowBlock.getPositionCount(), StandardErrorCode.INTERNAL_ERROR, "Expected row value field count does not match type field count");

            List<Object> row = new ArrayList<>(rowBlock.getPositionCount());
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                Object element = getField(fieldTypes.get(i), rowBlock, i);
                row.add(element);
            }

            return Collections.unmodifiableList(row);
        }
        throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    public static void checkTableIsWritable(Table table)
    {
        checkWritable(
                new SchemaTableName(table.getDbName(), table.getTableName()),
                Optional.empty(),
                getProtectMode(table),
                table.getParameters(),
                table.getSd());
    }

    public static void checkPartitionIsWritable(String partitionName, Partition partition)
    {
        checkWritable(
                new SchemaTableName(partition.getDbName(), partition.getTableName()),
                Optional.of(partitionName),
                getProtectMode(partition),
                partition.getParameters(),
                partition.getSd());
    }

    private static void checkWritable(
            SchemaTableName tableName,
            Optional<String> partitionName,
            ProtectMode protectMode,
            Map<String, String> parameters,
            StorageDescriptor storageDescriptor)
    {
        String tablePartitionDescription = "Table '" + tableName + "'";
        if (partitionName.isPresent()) {
            tablePartitionDescription += " partition '" + partitionName.get() + "'";
        }

        // verify online
        if (protectMode.offline) {
            throw new TableOfflineException(tableName, format("%s is offline", tablePartitionDescription));
        }

        String prestoOffline = parameters.get(PRESTO_OFFLINE);
        if (!isNullOrEmpty(prestoOffline)) {
            throw new TableOfflineException(tableName, format("%s is offline for Presto: %s", tablePartitionDescription, prestoOffline));
        }

        // verify not read only
        if (protectMode.readOnly) {
            throw new HiveReadOnlyException(tableName, partitionName);
        }

        // verify storage descriptor is valid
        if (storageDescriptor == null) {
            throw new PrestoException(HIVE_INVALID_METADATA, format("%s does not contain a valid storage descriptor", tablePartitionDescription));
        }

        // verify bucketing
        List<String> bucketColumns = storageDescriptor.getBucketCols();
        if (bucketColumns != null && !bucketColumns.isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, format("Inserting into bucketed tables is not supported. %s", tablePartitionDescription));
        }

        // verify sorting
        List<Order> sortColumns = storageDescriptor.getSortCols();
        if (sortColumns != null && !sortColumns.isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, format("Inserting into bucketed sorted tables is not supported. %s", tablePartitionDescription));
        }

        // verify skew info
        SkewedInfo skewedInfo = storageDescriptor.getSkewedInfo();
        if (skewedInfo != null && skewedInfo.getSkewedColNames() != null && !skewedInfo.getSkewedColNames().isEmpty()) {
            throw new PrestoException(NOT_SUPPORTED, format("Inserting into bucketed tables with skew is not supported. %s", tablePartitionDescription));
        }
    }

    public static Path getTableDefaultLocation(HiveMetastore metastore, HdfsEnvironment hdfsEnvironment, String schemaName, String tableName)
    {
        String location = getDatabase(metastore, schemaName).getLocationUri();
        if (isNullOrEmpty(location)) {
            throw new PrestoException(HIVE_DATABASE_LOCATION_ERROR, format("Database '%s' location is not set", schemaName));
        }

        Path databasePath = new Path(location);
        if (!pathExists(hdfsEnvironment, databasePath)) {
            throw new PrestoException(HIVE_DATABASE_LOCATION_ERROR, format("Database '%s' location does not exist: %s", schemaName, databasePath));
        }
        if (!isDirectory(hdfsEnvironment, databasePath)) {
            throw new PrestoException(HIVE_DATABASE_LOCATION_ERROR, format("Database '%s' location is not a directory: %s", schemaName, databasePath));
        }

        return new Path(databasePath, tableName);
    }

    private static Database getDatabase(HiveMetastore metastore, String database)
    {
        return metastore.getDatabase(database).orElseThrow(() -> new SchemaNotFoundException(database));
    }

    public static boolean pathExists(HdfsEnvironment hdfsEnvironment, Path path)
    {
        try {
            return hdfsEnvironment.getFileSystem(path).exists(path);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    private static boolean isDirectory(HdfsEnvironment hdfsEnvironment, Path path)
    {
        try {
            return hdfsEnvironment.getFileSystem(path).isDirectory(path);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    public static void renameDirectory(HdfsEnvironment hdfsEnvironment, String schemaName, String tableName, Path source, Path target)
    {
        if (pathExists(hdfsEnvironment, target)) {
            throw new PrestoException(HIVE_PATH_ALREADY_EXISTS,
                    format("Unable to commit creation of table '%s.%s': target directory already exists: %s", schemaName, tableName, target));
        }

        if (!pathExists(hdfsEnvironment, target.getParent())) {
            createDirectory(hdfsEnvironment, target.getParent());
        }

        try {
            if (!hdfsEnvironment.getFileSystem(source).rename(source, target)) {
                throw new PrestoException(HIVE_FILESYSTEM_ERROR, format("Failed to rename %s to %s: rename returned false", source, target));
            }
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, format("Failed to rename %s to %s", source, target), e);
        }
    }

    public static String createTemporaryPath(HdfsEnvironment hdfsEnvironment, Path targetPath)
    {
        // use a per-user temporary directory to avoid permission problems
        // TODO: this should use Hadoop UserGroupInformation
        String temporaryPrefix = "/tmp/presto-" + StandardSystemProperty.USER_NAME.value();

        // create a temporary directory on the same filesystem
        Path temporaryRoot = new Path(targetPath, temporaryPrefix);
        Path temporaryPath = new Path(temporaryRoot, randomUUID().toString());

        createDirectory(hdfsEnvironment, temporaryPath);

        return temporaryPath.toString();
    }

    public static void createDirectory(HdfsEnvironment hdfsEnvironment, Path path)
    {
        try {
            if (!hdfsEnvironment.getFileSystem(path).mkdirs(path, ALL_PERMISSIONS)) {
                throw new IOException("mkdirs returned false");
            }
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed to create directory: " + path, e);
        }

        // explicitly set permission since the default umask overrides it on creation
        try {
            hdfsEnvironment.getFileSystem(path).setPermission(path, ALL_PERMISSIONS);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed to set permission on directory: " + path, e);
        }
    }
}
