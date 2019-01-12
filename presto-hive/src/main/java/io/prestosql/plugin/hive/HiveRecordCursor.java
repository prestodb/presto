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
package io.prestosql.plugin.hive;

import com.google.common.annotations.VisibleForTesting;
import io.airlift.slice.Slice;
import io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMapping;
import io.prestosql.spi.PrestoException;
import io.prestosql.spi.connector.RecordCursor;
import io.prestosql.spi.type.DecimalType;
import io.prestosql.spi.type.Type;
import io.prestosql.spi.type.TypeManager;
import org.joda.time.DateTimeZone;

import java.util.List;

import static io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMappingKind.PREFILLED;
import static io.prestosql.plugin.hive.HivePageSourceProvider.ColumnMappingKind.REGULAR;
import static io.prestosql.plugin.hive.HiveUtil.bigintPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.booleanPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.charPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.datePartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.doublePartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.floatPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.integerPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.longDecimalPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.shortDecimalPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.smallintPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.timestampPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.tinyintPartitionKey;
import static io.prestosql.plugin.hive.HiveUtil.varcharPartitionKey;
import static io.prestosql.spi.StandardErrorCode.NOT_SUPPORTED;
import static io.prestosql.spi.type.BigintType.BIGINT;
import static io.prestosql.spi.type.BooleanType.BOOLEAN;
import static io.prestosql.spi.type.Chars.isCharType;
import static io.prestosql.spi.type.DateType.DATE;
import static io.prestosql.spi.type.Decimals.isLongDecimal;
import static io.prestosql.spi.type.Decimals.isShortDecimal;
import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static io.prestosql.spi.type.RealType.REAL;
import static io.prestosql.spi.type.SmallintType.SMALLINT;
import static io.prestosql.spi.type.TimestampType.TIMESTAMP;
import static io.prestosql.spi.type.TinyintType.TINYINT;
import static io.prestosql.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class HiveRecordCursor
        implements RecordCursor
{
    private final RecordCursor delegate;

    private final List<ColumnMapping> columnMappings;
    private final Type[] types;

    private final boolean[] booleans;
    private final long[] longs;
    private final double[] doubles;
    private final Slice[] slices;
    private final Object[] objects;
    private final boolean[] nulls;

    public HiveRecordCursor(
            List<ColumnMapping> columnMappings,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            RecordCursor delegate)
    {
        requireNonNull(columnMappings, "columns is null");
        requireNonNull(typeManager, "typeManager is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.columnMappings = columnMappings;

        int size = columnMappings.size();

        this.types = new Type[size];

        this.booleans = new boolean[size];
        this.longs = new long[size];
        this.doubles = new double[size];
        this.slices = new Slice[size];
        this.objects = new Object[size];
        this.nulls = new boolean[size];

        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            ColumnMapping columnMapping = columnMappings.get(columnIndex);

            if (columnMapping.getKind() == PREFILLED) {
                String columnValue = columnMapping.getPrefilledValue();
                byte[] bytes = columnValue.getBytes(UTF_8);

                String name = columnMapping.getHiveColumnHandle().getName();
                Type type = typeManager.getType(columnMapping.getHiveColumnHandle().getTypeSignature());
                types[columnIndex] = type;

                if (HiveUtil.isHiveNull(bytes)) {
                    nulls[columnIndex] = true;
                }
                else if (BOOLEAN.equals(type)) {
                    booleans[columnIndex] = booleanPartitionKey(columnValue, name);
                }
                else if (TINYINT.equals(type)) {
                    longs[columnIndex] = tinyintPartitionKey(columnValue, name);
                }
                else if (SMALLINT.equals(type)) {
                    longs[columnIndex] = smallintPartitionKey(columnValue, name);
                }
                else if (INTEGER.equals(type)) {
                    longs[columnIndex] = integerPartitionKey(columnValue, name);
                }
                else if (BIGINT.equals(type)) {
                    longs[columnIndex] = bigintPartitionKey(columnValue, name);
                }
                else if (REAL.equals(type)) {
                    longs[columnIndex] = floatPartitionKey(columnValue, name);
                }
                else if (DOUBLE.equals(type)) {
                    doubles[columnIndex] = doublePartitionKey(columnValue, name);
                }
                else if (isVarcharType(type)) {
                    slices[columnIndex] = varcharPartitionKey(columnValue, name, type);
                }
                else if (isCharType(type)) {
                    slices[columnIndex] = charPartitionKey(columnValue, name, type);
                }
                else if (DATE.equals(type)) {
                    longs[columnIndex] = datePartitionKey(columnValue, name);
                }
                else if (TIMESTAMP.equals(type)) {
                    longs[columnIndex] = timestampPartitionKey(columnValue, hiveStorageTimeZone, name);
                }
                else if (isShortDecimal(type)) {
                    longs[columnIndex] = shortDecimalPartitionKey(columnValue, (DecimalType) type, name);
                }
                else if (isLongDecimal(type)) {
                    slices[columnIndex] = longDecimalPartitionKey(columnValue, (DecimalType) type, name);
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for prefilled column: %s", type.getDisplayName(), name));
                }
            }
        }
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public Type getType(int field)
    {
        return types[field];
    }

    @Override
    public boolean advanceNextPosition()
    {
        return delegate.advanceNextPosition();
    }

    @Override
    public boolean getBoolean(int field)
    {
        ColumnMapping columnMapping = columnMappings.get(field);
        if (columnMapping.getKind() == REGULAR) {
            return delegate.getBoolean(columnMapping.getIndex());
        }
        return booleans[field];
    }

    @Override
    public long getLong(int field)
    {
        ColumnMapping columnMapping = columnMappings.get(field);
        if (columnMapping.getKind() == REGULAR) {
            return delegate.getLong(columnMapping.getIndex());
        }
        return longs[field];
    }

    @Override
    public double getDouble(int field)
    {
        ColumnMapping columnMapping = columnMappings.get(field);
        if (columnMapping.getKind() == REGULAR) {
            return delegate.getDouble(columnMapping.getIndex());
        }
        return doubles[field];
    }

    @Override
    public Slice getSlice(int field)
    {
        ColumnMapping columnMapping = columnMappings.get(field);
        if (columnMapping.getKind() == REGULAR) {
            return delegate.getSlice(columnMapping.getIndex());
        }
        return slices[field];
    }

    @Override
    public Object getObject(int field)
    {
        ColumnMapping columnMapping = columnMappings.get(field);
        if (columnMapping.getKind() == REGULAR) {
            return delegate.getObject(columnMapping.getIndex());
        }
        return objects[field];
    }

    @Override
    public boolean isNull(int field)
    {
        ColumnMapping columnMapping = columnMappings.get(field);
        if (columnMapping.getKind() == REGULAR) {
            return delegate.isNull(columnMapping.getIndex());
        }
        return nulls[field];
    }

    @Override
    public void close()
    {
        delegate.close();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    @VisibleForTesting
    RecordCursor getRegularColumnRecordCursor()
    {
        return delegate;
    }
}
