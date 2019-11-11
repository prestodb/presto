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

import com.facebook.presto.hive.HdfsEnvironment.HdfsContext;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.StandardErrorCode;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.BooleanType;
import com.facebook.presto.spi.type.CharType;
import com.facebook.presto.spi.type.DateType;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Decimals;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.IntegerType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.SmallintType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.TinyintType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.VarbinaryType;
import com.facebook.presto.spi.type.VarcharType;
import com.google.common.base.CharMatcher;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.fs.permission.FsPermission;
import org.apache.hadoop.hive.common.type.HiveDecimal;
import org.apache.hadoop.io.Text;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.math.BigInteger;
import java.sql.Date;
import java.sql.Timestamp;
import java.util.ArrayList;
import java.util.Collections;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.hive.HivePartitionKey.HIVE_DEFAULT_DYNAMIC_PARTITION;
import static com.facebook.presto.hive.MetastoreErrorCode.HIVE_FILESYSTEM_ERROR;
import static com.facebook.presto.hive.MetastoreErrorCode.HIVE_INVALID_PARTITION_VALUE;
import static com.facebook.presto.hive.metastore.MetastoreUtil.checkCondition;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isArrayType;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isMapType;
import static com.facebook.presto.hive.metastore.MetastoreUtil.isRowType;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.google.common.base.Strings.padEnd;
import static com.google.common.io.BaseEncoding.base16;
import static java.lang.Float.intBitsToFloat;
import static java.nio.charset.StandardCharsets.UTF_8;
import static org.joda.time.DateTimeZone.UTC;

public final class MetastoreWriteUtil
{
    @SuppressWarnings("OctalInteger")
    private static final FsPermission ALL_PERMISSIONS = new FsPermission((short) 0777);

    private MetastoreWriteUtil()
    {
    }

    public static boolean pathExists(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path)
    {
        try {
            return hdfsEnvironment.getFileSystem(context, path).exists(path);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed checking path: " + path, e);
        }
    }

    public static void createDirectory(HdfsContext context, HdfsEnvironment hdfsEnvironment, Path path)
    {
        try {
            if (!hdfsEnvironment.getFileSystem(context, path).mkdirs(path, ALL_PERMISSIONS)) {
                throw new IOException("mkdirs returned false");
            }
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed to create directory: " + path, e);
        }

        // explicitly set permission since the default umask overrides it on creation
        try {
            hdfsEnvironment.getFileSystem(context, path).setPermission(path, ALL_PERMISSIONS);
        }
        catch (IOException e) {
            throw new PrestoException(HIVE_FILESYSTEM_ERROR, "Failed to set permission on directory: " + path, e);
        }
    }

    public static List<String> createPartitionValues(List<Type> partitionColumnTypes, Page partitionColumns, int position)
    {
        ImmutableList.Builder<String> partitionValues = ImmutableList.builder();
        for (int field = 0; field < partitionColumns.getChannelCount(); field++) {
            Object value = getField(partitionColumnTypes.get(field), partitionColumns.getBlock(field), position);
            if (value == null) {
                partitionValues.add(HIVE_DEFAULT_DYNAMIC_PARTITION);
            }
            else {
                String valueString = value.toString();
                if (!CharMatcher.inRange((char) 0x20, (char) 0x7E).matchesAllOf(valueString)) {
                    throw new PrestoException(HIVE_INVALID_PARTITION_VALUE,
                            "Hive partition keys can only contain printable ASCII characters (0x20 - 0x7E). Invalid value: " +
                                    base16().withSeparator(" ", 2).encode(valueString.getBytes(UTF_8)));
                }
                partitionValues.add(valueString);
            }
        }
        return partitionValues.build();
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
        if (IntegerType.INTEGER.equals(type)) {
            return (int) type.getLong(block, position);
        }
        if (SmallintType.SMALLINT.equals(type)) {
            return (short) type.getLong(block, position);
        }
        if (TinyintType.TINYINT.equals(type)) {
            return (byte) type.getLong(block, position);
        }
        if (RealType.REAL.equals(type)) {
            return intBitsToFloat((int) type.getLong(block, position));
        }
        if (DoubleType.DOUBLE.equals(type)) {
            return type.getDouble(block, position);
        }
        if (type instanceof VarcharType) {
            return new Text(type.getSlice(block, position).getBytes());
        }
        if (type instanceof CharType) {
            CharType charType = (CharType) type;
            return new Text(padEnd(type.getSlice(block, position).toStringUtf8(), charType.getLength(), ' '));
        }
        if (VarbinaryType.VARBINARY.equals(type)) {
            return type.getSlice(block, position).getBytes();
        }
        if (DateType.DATE.equals(type)) {
            long days = type.getLong(block, position);
            return new Date(UTC.getMillisKeepLocal(DateTimeZone.getDefault(), TimeUnit.DAYS.toMillis(days)));
        }
        if (TimestampType.TIMESTAMP.equals(type)) {
            long millisUtc = type.getLong(block, position);
            return new Timestamp(millisUtc);
        }
        if (type instanceof DecimalType) {
            DecimalType decimalType = (DecimalType) type;
            return getHiveDecimal(decimalType, block, position);
        }
        if (isArrayType(type)) {
            Type elementType = type.getTypeParameters().get(0);

            Block arrayBlock = block.getBlock(position);

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

            Block mapBlock = block.getBlock(position);
            Map<Object, Object> map = new HashMap<>();
            for (int i = 0; i < mapBlock.getPositionCount(); i += 2) {
                Object key = getField(keyType, mapBlock, i);
                Object value = getField(valueType, mapBlock, i + 1);
                map.put(key, value);
            }

            return Collections.unmodifiableMap(map);
        }
        if (isRowType(type)) {
            Block rowBlock = block.getBlock(position);

            List<Type> fieldTypes = type.getTypeParameters();
            checkCondition(fieldTypes.size() == rowBlock.getPositionCount(), StandardErrorCode.GENERIC_INTERNAL_ERROR, "Expected row value field count does not match type field count");

            List<Object> row = new ArrayList<>(rowBlock.getPositionCount());
            for (int i = 0; i < rowBlock.getPositionCount(); i++) {
                Object element = getField(fieldTypes.get(i), rowBlock, i);
                row.add(element);
            }

            return Collections.unmodifiableList(row);
        }
        throw new PrestoException(NOT_SUPPORTED, "unsupported type: " + type);
    }

    public static HiveDecimal getHiveDecimal(DecimalType decimalType, Block block, int position)
    {
        BigInteger unscaledValue;
        if (decimalType.isShort()) {
            unscaledValue = BigInteger.valueOf(decimalType.getLong(block, position));
        }
        else {
            unscaledValue = Decimals.decodeUnscaledValue(decimalType.getSlice(block, position));
        }
        return HiveDecimal.create(unscaledValue, decimalType.getScale());
    }
}
