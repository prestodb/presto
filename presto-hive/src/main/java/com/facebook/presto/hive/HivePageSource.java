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

import com.facebook.presto.hive.HivePageSourceProvider.ColumnMapping;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.block.RunLengthEncodedBlock;
import com.facebook.presto.spi.type.DecimalType;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.google.common.base.Throwables;
import org.joda.time.DateTimeZone;

import java.io.IOException;
import java.util.List;

import static com.facebook.presto.hive.HiveErrorCode.HIVE_CURSOR_ERROR;
import static com.facebook.presto.hive.HiveUtil.bigintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.booleanPartitionKey;
import static com.facebook.presto.hive.HiveUtil.charPartitionKey;
import static com.facebook.presto.hive.HiveUtil.datePartitionKey;
import static com.facebook.presto.hive.HiveUtil.doublePartitionKey;
import static com.facebook.presto.hive.HiveUtil.floatPartitionKey;
import static com.facebook.presto.hive.HiveUtil.integerPartitionKey;
import static com.facebook.presto.hive.HiveUtil.longDecimalPartitionKey;
import static com.facebook.presto.hive.HiveUtil.shortDecimalPartitionKey;
import static com.facebook.presto.hive.HiveUtil.smallintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.timestampPartitionKey;
import static com.facebook.presto.hive.HiveUtil.tinyintPartitionKey;
import static com.facebook.presto.hive.HiveUtil.varcharPartitionKey;
import static com.facebook.presto.spi.StandardErrorCode.NOT_SUPPORTED;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.Chars.isCharType;
import static com.facebook.presto.spi.type.DateType.DATE;
import static com.facebook.presto.spi.type.Decimals.isLongDecimal;
import static com.facebook.presto.spi.type.Decimals.isShortDecimal;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.RealType.REAL;
import static com.facebook.presto.spi.type.SmallintType.SMALLINT;
import static com.facebook.presto.spi.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.spi.type.TinyintType.TINYINT;
import static com.facebook.presto.spi.type.Varchars.isVarcharType;
import static java.lang.String.format;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class HivePageSource
        implements ConnectorPageSource
{
    private List<ColumnMapping> columnMappings;
    private final Object[] prefilledValues;
    private Type[] types;

    private final ConnectorPageSource delegate;

    public HivePageSource(
            List<ColumnMapping> columnMappings,
            DateTimeZone hiveStorageTimeZone,
            TypeManager typeManager,
            ConnectorPageSource delegate)
    {
        requireNonNull(columnMappings, "columnMappings is null");
        requireNonNull(hiveStorageTimeZone, "hiveStorageTimeZone is null");
        requireNonNull(typeManager, "typeManager is null");

        this.delegate = requireNonNull(delegate, "delegate is null");
        this.columnMappings = columnMappings;

        int size = columnMappings.size();

        prefilledValues = new Object[size];
        types = new Type[size];

        for (int columnIndex = 0; columnIndex < size; columnIndex++) {
            ColumnMapping columnMapping = columnMappings.get(columnIndex);
            HiveColumnHandle column = columnMapping.getHiveColumnHandle();

            String name = column.getName();
            Type type = typeManager.getType(column.getTypeSignature());
            types[columnIndex] = type;

            if (columnMapping.isPrefilled()) {
                String columnValue = columnMapping.getPrefilledValue();
                byte[] bytes = columnValue.getBytes(UTF_8);

                Object prefilledValue;
                if (HiveUtil.isHiveNull(bytes)) {
                    prefilledValue = null;
                }
                else if (type.equals(BOOLEAN)) {
                    prefilledValue = booleanPartitionKey(columnValue, name);
                }
                else if (type.equals(BIGINT)) {
                    prefilledValue = bigintPartitionKey(columnValue, name);
                }
                else if (type.equals(INTEGER)) {
                    prefilledValue = integerPartitionKey(columnValue, name);
                }
                else if (type.equals(SMALLINT)) {
                    prefilledValue = smallintPartitionKey(columnValue, name);
                }
                else if (type.equals(TINYINT)) {
                    prefilledValue = tinyintPartitionKey(columnValue, name);
                }
                else if (type.equals(REAL)) {
                    prefilledValue = floatPartitionKey(columnValue, name);
                }
                else if (type.equals(DOUBLE)) {
                    prefilledValue = doublePartitionKey(columnValue, name);
                }
                else if (isVarcharType(type)) {
                    prefilledValue = varcharPartitionKey(columnValue, name, type);
                }
                else if (isCharType(type)) {
                    prefilledValue = charPartitionKey(columnValue, name, type);
                }
                else if (type.equals(DATE)) {
                    prefilledValue = datePartitionKey(columnValue, name);
                }
                else if (type.equals(TIMESTAMP)) {
                    prefilledValue = timestampPartitionKey(columnValue, hiveStorageTimeZone, name);
                }
                else if (isShortDecimal(type)) {
                    prefilledValue = shortDecimalPartitionKey(columnValue, (DecimalType) type, name);
                }
                else if (isLongDecimal(type)) {
                    prefilledValue = longDecimalPartitionKey(columnValue, (DecimalType) type, name);
                }
                else {
                    throw new PrestoException(NOT_SUPPORTED, format("Unsupported column type %s for prefilled column: %s", type.getDisplayName(), name));
                }

                prefilledValues[columnIndex] = prefilledValue;
            }
        }
    }

    @Override
    public long getTotalBytes()
    {
        return delegate.getTotalBytes();
    }

    @Override
    public long getCompletedBytes()
    {
        return delegate.getCompletedBytes();
    }

    @Override
    public long getReadTimeNanos()
    {
        return delegate.getReadTimeNanos();
    }

    @Override
    public boolean isFinished()
    {
        return delegate.isFinished();
    }

    @Override
    public Page getNextPage()
    {
        try {
            Block[] blocks = new Block[columnMappings.size()];
            Page dataPage = delegate.getNextPage();
            if (dataPage == null) {
                return null;
            }
            int batchSize = dataPage.getPositionCount();
            for (int fieldId = 0; fieldId < blocks.length; fieldId++) {
                if (columnMappings.get(fieldId).isPrefilled()) {
                    blocks[fieldId] = RunLengthEncodedBlock.create(types[fieldId], prefilledValues[fieldId], batchSize);
                }
                else {
                    blocks[fieldId] = dataPage.getBlock(columnMappings.get(fieldId).getIndex());
                }
            }
            return new Page(batchSize, blocks);
        }
        catch (PrestoException e) {
            closeWithSuppression(e);
            throw e;
        }
        catch (RuntimeException e) {
            closeWithSuppression(e);
            throw new PrestoException(HIVE_CURSOR_ERROR, e);
        }
    }

    @Override
    public void close()
    {
        try {
            delegate.close();
        }
        catch (IOException e) {
            throw Throwables.propagate(e);
        }
    }

    @Override
    public String toString()
    {
        return delegate.toString();
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return delegate.getSystemMemoryUsage();
    }

    protected void closeWithSuppression(Throwable throwable)
    {
        requireNonNull(throwable, "throwable is null");
        try {
            close();
        }
        catch (RuntimeException e) {
            // Self-suppression not permitted
            if (throwable != e) {
                throwable.addSuppressed(e);
            }
        }
    }

    public ConnectorPageSource getPageSource()
    {
        return delegate;
    }
}
