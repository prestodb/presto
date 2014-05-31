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

import com.facebook.presto.spi.RecordSink;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import java.util.List;
import java.util.UUID;

import static com.facebook.presto.cassandra.CassandraColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;

public class CassandraRecordSink implements RecordSink
{
    private final int fieldCount;
    private final CassandraSession cassandraSession;
    private final boolean sampled;
    private final String insertQuery;
    private final List<Object> values;
    private int field = -1;

    @Inject
    public CassandraRecordSink(CassandraOutputTableHandle handle, CassandraSession cassandraSession)
    {
        this.fieldCount = checkNotNull(handle, "handle is null").getColumnNames().size();
        this.cassandraSession = checkNotNull(cassandraSession, "cassandraSession is null");
        this.sampled = handle.isSampled();

        StringBuilder queryBuilder = new StringBuilder(String.format("INSERT INTO \"%s\".\"%s\"(", handle.getSchemaName(), handle.getTableName()));
        queryBuilder.append("id");

        if (sampled) {
            queryBuilder.append("," + SAMPLE_WEIGHT_COLUMN_NAME);
        }

        for (String columnName : handle.getColumnNames()) {
            queryBuilder.append(",").append(columnName);
        }
        queryBuilder.append(") VALUES (?");

        if (sampled) {
            queryBuilder.append(",?");
        }

        for (int i = 0; i < handle.getColumnNames().size(); i++) {
            queryBuilder.append(",?");
        }
        queryBuilder.append(")");

        insertQuery = queryBuilder.toString();
        values = Lists.newArrayList();
    }

    @Override
    public void beginRecord(long sampleWeight)
    {
        checkState(field == -1, "already in record");

        field = 0;
        values.clear();
        values.add(UUID.randomUUID());

        if (sampled) {
            values.add(sampleWeight);
        }
        else {
            checkArgument(sampleWeight == 1, "Sample weight must be 1 when sampling is disabled");
        }
    }

    @Override
    public void finishRecord()
    {
        checkState(field != -1, "not in record");
        checkState(field == fieldCount, "not all fields set");
        field = -1;
        cassandraSession.execute(insertQuery, values.toArray());
    }

    @Override
    public void appendNull()
    {
        append(null);
    }

    @Override
    public void appendBoolean(boolean value)
    {
        append(value);
    }

    @Override
    public void appendLong(long value)
    {
        append(value);
    }

    @Override
    public void appendDouble(double value)
    {
        append(value);
    }

    @Override
    public void appendString(byte[] value)
    {
        append(new String(value, UTF_8));
    }

    @Override
    public String commit()
    {
        checkState(field == -1, "record not finished");
        return "";
    }

    private void append(Object value)
    {
        checkState(field != -1, "not in record");
        checkState(field < fieldCount, "all fields already set");
        values.add(value);
        field++;
    }
}
