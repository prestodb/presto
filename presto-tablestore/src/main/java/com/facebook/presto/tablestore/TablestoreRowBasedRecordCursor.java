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
package com.facebook.presto.tablestore;

import com.alicloud.openservices.tablestore.model.Column;
import com.alicloud.openservices.tablestore.model.ColumnValue;
import com.alicloud.openservices.tablestore.model.PrimaryKeyValue;
import com.alicloud.openservices.tablestore.model.Row;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.IntegerType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.util.Iterator;
import java.util.List;
import java.util.Optional;

import static com.alicloud.openservices.tablestore.model.ColumnValue.INTERNAL_NULL_VALUE;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.common.base.Preconditions.checkArgument;

public abstract class TablestoreRowBasedRecordCursor
        extends TablestoreAbstractRecordCursor
        implements RecordCursor
{
    protected final Iterator<Row> rowIterator;
    protected Row current;

    public TablestoreRowBasedRecordCursor(TablestoreFacade tablestoreFacade, ConnectorSession session, TablestoreSplit split,
            List<TablestoreColumnHandle> columnHandles)
    {
        super(tablestoreFacade, session, split, columnHandles);
        WrappedRowIterator wrappedRowIterator = fetchFirst();
        rowIterator = wrappedRowIterator.getIterator();
        apiInNanos += wrappedRowIterator.getApiCostNanos();
    }

    public abstract WrappedRowIterator fetchFirst();

    public abstract long getCompletedBytes();

    @Override
    protected boolean hasNext()
    {
        return rowIterator.hasNext();
    }

    @Override
    protected void next()
    {
        current = rowIterator.next();
    }

    @Override
    protected String currentPrimaryKeyInfo()
    {
        return current.getPrimaryKey().toString();
    }

    @Override
    public boolean getBoolean(int field)
    {
        return callAndRecord(field, () -> {
            if (isPrimaryKey(field)) {
                return Boolean.valueOf(locatePk(field).asString());
            }
            else {
                return locateCvNonnull(field).asBoolean();
            }
        });
    }

    private PrimaryKeyValue locatePk(int field)
    {
        return current.getPrimaryKey().getPrimaryKeyColumn(getColumnName(field)).getValue();
    }

    private ColumnValue locateCvNonnull(int field)
    {
        String name = getColumnName(field);
        List<Column> columns = current.getColumn(name);
        checkArgument(!columns.isEmpty(), "Can't get any column value of the field[index=%s,name=%s]", field, name);
        return columns.get(0).getValue();
    }

    private Optional<ColumnValue> locateCvNullable(int field)
    {
        String name = getColumnName(field);
        List<Column> columns = current.getColumn(name);
        return columns.isEmpty() ? Optional.empty() : Optional.ofNullable(columns.get(0).getValue());
    }

    private long asLong(int field)
    {
        if (isPrimaryKey(field)) {
            return locatePk(field).asLong();
        }
        else {
            return locateCvNonnull(field).asLong();
        }
    }

    @Override
    public long getLong(int field)
    {
        return callAndRecord(field, () -> {
            Type type = getType(field);
            if (type.equals(BigintType.BIGINT)) {
                return asLong(field);
            }
            if (type.equals(IntegerType.INTEGER)) {
                return asLong(field);
            }

            String columnName = getColumnName(field);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type of column[columnName=" + columnName
                    + "] for long: " + type.getTypeSignature());
        });
    }

    @Override
    public double getDouble(int field)
    {
        return callAndRecord(field, () -> {
            if (isPrimaryKey(field)) {
                return Double.valueOf(locatePk(field).asString());
            }
            else {
                return locateCvNonnull(field).asDouble();
            }
        });
    }

    @Override
    public Slice getSlice(int field)
    {
        return callAndRecord(field, () -> {
            Type type = getType(field);
            if (type instanceof VarcharType || type instanceof CharType) {
                if (isPrimaryKey(field)) {
                    return Slices.utf8Slice(locatePk(field).asString());
                }
                else {
                    return Slices.utf8Slice(locateCvNonnull(field).asString());
                }
            }
            if (type instanceof VarbinaryType) {
                if (isPrimaryKey(field)) {
                    return Slices.wrappedBuffer(locatePk(field).asBinary());
                }
                else {
                    return Slices.wrappedBuffer(locateCvNonnull(field).asBinary());
                }
            }
            String columnName = getColumnName(field);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unhandled type of column[columnName=" + columnName
                    + "] for slice: " + type.getTypeSignature());
        });
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public boolean isNull(int field)
    {
        return callAndRecord(field, () -> {
            if (isPrimaryKey(field)) {
                return false;
            }
            Optional<ColumnValue> columnValue = locateCvNullable(field);
            return !columnValue.isPresent() || columnValue.get().equals(INTERNAL_NULL_VALUE);
        });
    }
}
