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

import com.alicloud.openservices.tablestore.model.ColumnType;
import com.alicloud.openservices.tablestore.model.SimpleRowMatrixBlockRowIterator;
import com.facebook.presto.common.type.CharType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.VarbinaryType;
import com.facebook.presto.common.type.VarcharType;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.apache.commons.lang3.tuple.Triple;

import javax.annotation.Nonnull;

import java.util.List;

public class TablestoreRecordCursor4RangeBlock
        extends TablestoreAbstractRecordCursor
        implements RecordCursor
{
    private final SimpleRowMatrixBlockRowIterator rowIterator;

    public TablestoreRecordCursor4RangeBlock(TablestoreFacade tablestoreFacade, ConnectorSession session, TablestoreSplit split,
            List<TablestoreColumnHandle> columnHandles)
    {
        super(tablestoreFacade, session, split, columnHandles);
        Triple<SimpleRowMatrixBlockRowIterator, Long, Long> x = tablestoreFacade.rowIterator4RangeBlock(session, split, columnHandles);
        this.rowIterator = x.getLeft();
        apiInNanos += x.getRight();
    }

    @Override
    public long getCompletedBytes()
    {
        checkClosed();
        return rowIterator.getCompletedBytes();
    }

    @Override
    protected boolean hasNext()
    {
        return rowIterator.hasNext();
    }

    @Override
    protected void next()
    {
        rowIterator.next();
    }

    @Override
    protected String currentPrimaryKeyInfo()
    {
        return primaryKeys.stream().map(pk -> {
            String name = pk.getColumnName();
            ColumnType type = rowIterator.getFieldType(name);
            Object b = rowIterator.getObject(name);
            return name + "<" + type + ">:" + b;
        }).reduce("{", (a, b) -> a + ", " + b) + "}";
    }

    @Override
    public boolean getBoolean(int field)
    {
        Class<Boolean> clazz = boolean.class;
        return callAndRecord(field, () -> {
            String name = getColumnName(field);
            Object value = rowIterator.getObject(name);
            Type type = getType(field);
            if (value == null) {
                throw nonnull(name, type, clazz);
            }
            if (value instanceof Boolean) {
                return (Boolean) value;
            }
            if (value instanceof Long) {
                return (Long) value != 0;
            }
            if (value instanceof Double) {
                return ((Double) value).longValue() != 0;
            }
            if (value instanceof String) {
                String s = (String) value;
                if ("true".equalsIgnoreCase(s) || "1".equalsIgnoreCase(s)) {
                    return true;
                }
                else if ("false".equalsIgnoreCase(s) || "0".equalsIgnoreCase(s)) {
                    return false;
                }
                else {
                    throw nonMatchType(name, type, clazz, s);
                }
            }
            throw unsupportedType(name, type, clazz, value);
        });
    }

    @SuppressWarnings("SameParameterValue")
    private RuntimeException nonMatchType(String name, Type type, Class expectedType, Object value)
    {
        return new RuntimeException("Field[" + name + "] and value[" + toString(value) +
                "], expected java type<" + expectedType + "> and MPP type<" + type.getTypeSignature() +
                "> doesn't match actual type<" + value.getClass() +
                ">, we can't parse the value to expected type, PK=" + currentPrimaryKeyInfo());
    }

    private RuntimeException unsupportedType(String name, Type type, Class expectedType, @Nonnull Object value)
    {
        return new RuntimeException("Field[" + name + "] and value[" + toString(value) +
                "], expected java type<" + expectedType + "> and MPP type<" + type.getTypeSignature() +
                "> doesn't match actual type<" + value.getClass() + ">, PK=" +
                currentPrimaryKeyInfo());
    }

    private RuntimeException nonnull(String name, Type type, Class expectedType)
    {
        return new RuntimeException("The value of field[" + name + "] is null " +
                "but expected java type<" + expectedType + "> and MPP type<" + type.getTypeSignature() +
                ">, PK=" + currentPrimaryKeyInfo());
    }

    private RuntimeException parseFailed(String name, Object s, Class expectedType, Throwable e)
    {
        return new RuntimeException("Can't parse the object[" + toString(s) + "] to a " + expectedType +
                " for field[" + name + "], PK=" + currentPrimaryKeyInfo(), e);
    }

    @Override
    public long getLong(int field)
    {
        Class<Long> clazz = long.class;
        return callAndRecord(field, () -> {
            String name = getColumnName(field);
            Object value = rowIterator.getObject(name);
            Type type = getType(field);
            if (value == null) {
                throw nonnull(name, type, clazz);
            }
            if (value instanceof Long) {
                return (Long) value;
            }
            if (enableLooseCast) {
                if (value instanceof Double) {
                    return ((Double) value).longValue();
                }
                if (value instanceof Boolean) {
                    return (Boolean) value ? 1L : 0L;
                }
                if (value instanceof byte[]) {
                    try {
                        String s = new String((byte[]) value);
                        if (s.length() == 0) {
                            return 0L;
                        }
                        return Long.parseLong(s);
                    }
                    catch (Exception e) {
                        throw parseFailed(name, value, clazz, e);
                    }
                }
            }
            //STRING or BINARY
            if (value instanceof String) {
                String str = (String) value;
                if (str.length() == 0 && enableLooseCast) {
                    return 0L;
                }
                try {
                    return Long.parseLong(str);
                }
                catch (Exception e) {
                    throw parseFailed(name, str, clazz, e);
                }
            }
            throw unsupportedType(name, type, clazz, value);
        });
    }

    @Override
    public double getDouble(int field)
    {
        Class<Double> c = double.class;
        return callAndRecord(field, () -> {
            String name = getColumnName(field);
            Object b = rowIterator.getObject(name);
            Type type = getType(field);
            if (b == null) {
                throw nonnull(name, type, c);
            }
            if (b instanceof Double) {
                return (Double) b;
            }
            if (enableLooseCast) {
                if (b instanceof Long) {
                    return ((Long) b).doubleValue();
                }
                if (b instanceof Boolean) {
                    return (Boolean) b ? 1.0 : 0.0;
                }
                if (b instanceof byte[]) {
                    if (((byte[]) b).length == 0) {
                        return 0.0;
                    }
                    try {
                        String s = new String((byte[]) b);
                        return Double.parseDouble(s);
                    }
                    catch (Exception e) {
                        throw parseFailed(name, b, c, e);
                    }
                }
            }
            //STRING or BINARY
            if (b instanceof String) {
                String s = (String) b;
                if (s.length() == 0 && enableLooseCast) {
                    return 0.0;
                }
                try {
                    return Double.parseDouble(s);
                }
                catch (Exception e) {
                    throw parseFailed(name, s, c, e);
                }
            }
            throw unsupportedType(name, type, c, b);
        });
    }

    @Override
    public Slice getSlice(int field)
    {
        return callAndRecord(field, () -> {
            String name = getColumnName(field);
            Object value = rowIterator.getObject(name);
            Type type = getType(field);
            if (value == null) {
                throw nonnull(name, type, Slice.class);
            }

            if (type instanceof VarcharType || type instanceof CharType) {
                if (value instanceof Boolean || value instanceof Long || value instanceof Double || value instanceof String) {
                    return Slices.utf8Slice(String.valueOf(value));
                }
                if (value instanceof byte[]) {
                    return Slices.utf8Slice(new String((byte[]) value));
                }
            }
            if (type instanceof VarbinaryType) {
                if (value instanceof String) {
                    return Slices.wrappedBuffer(((String) value).getBytes());
                }
                if (value instanceof byte[]) {
                    return Slices.wrappedBuffer((byte[]) value);
                }
            }
            throw unsupportedType(name, type, Slices.class, value);
        });
    }

    @Override
    public Object getObject(int field)
    {
        return callAndRecord(field, () -> rowIterator.getObject(getColumnName(field)));
    }

    @Override
    public boolean isNull(int field)
    {
        return callAndRecord(field, () -> rowIterator.isNull(getColumnName(field)));
    }
}
