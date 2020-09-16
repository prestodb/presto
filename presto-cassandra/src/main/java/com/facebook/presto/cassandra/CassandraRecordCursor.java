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
package com.facebook.presto.cassandra;

import com.datastax.driver.core.ResultSet;
import com.datastax.driver.core.Row;
import com.facebook.presto.common.predicate.NullableValue;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.RecordCursor;
import io.airlift.slice.Slice;

import java.util.List;

import static io.airlift.slice.Slices.utf8Slice;
import static java.lang.Float.floatToRawIntBits;

public class CassandraRecordCursor
        implements RecordCursor
{
    private final List<FullCassandraType> fullCassandraTypes;
    private final ResultSet rs;
    private Row currentRow;
    private long count;

    public CassandraRecordCursor(CassandraSession cassandraSession, List<FullCassandraType> fullCassandraTypes, String cql)
    {
        this.fullCassandraTypes = fullCassandraTypes;
        rs = cassandraSession.execute(cql);
        currentRow = null;
    }

    @Override
    public boolean advanceNextPosition()
    {
        if (!rs.isExhausted()) {
            currentRow = rs.one();
            count++;
            return true;
        }
        return false;
    }

    @Override
    public void close()
    {
    }

    @Override
    public boolean getBoolean(int i)
    {
        return currentRow.getBool(i);
    }

    @Override
    public long getCompletedBytes()
    {
        return count;
    }

    @Override
    public long getReadTimeNanos()
    {
        return 0;
    }

    @Override
    public double getDouble(int i)
    {
        switch (getCassandraType(i)) {
            case DOUBLE:
                return currentRow.getDouble(i);
            case FLOAT:
                return currentRow.getFloat(i);
            case DECIMAL:
                return currentRow.getDecimal(i).doubleValue();
            default:
                throw new IllegalStateException("Cannot retrieve double for " + getCassandraType(i));
        }
    }

    @Override
    public long getLong(int i)
    {
        switch (getCassandraType(i)) {
            case INT:
                return currentRow.getInt(i);
            case SMALLINT:
                return currentRow.getShort(i);
            case TINYINT:
                return currentRow.getByte(i);
            case BIGINT:
            case COUNTER:
                return currentRow.getLong(i);
            case TIMESTAMP:
                return currentRow.getTimestamp(i).getTime();
            case DATE:
                return currentRow.getDate(i).getDaysSinceEpoch();
            case FLOAT:
                return floatToRawIntBits(currentRow.getFloat(i));
            default:
                throw new IllegalStateException("Cannot retrieve long for " + getCassandraType(i));
        }
    }

    private CassandraType getCassandraType(int i)
    {
        return fullCassandraTypes.get(i).getCassandraType();
    }

    @Override
    public Slice getSlice(int i)
    {
        NullableValue value = CassandraType.getColumnValue(currentRow, i, fullCassandraTypes.get(i));
        if (value.getValue() instanceof Slice) {
            return (Slice) value.getValue();
        }
        return utf8Slice(value.getValue().toString());
    }

    @Override
    public Object getObject(int field)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Type getType(int i)
    {
        return getCassandraType(i).getNativeType();
    }

    @Override
    public boolean isNull(int i)
    {
        return currentRow.isNull(i);
    }
}
