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

import java.util.List;
import java.util.UUID;

import com.facebook.presto.spi.RecordSink;
import com.google.common.collect.Lists;
import com.google.inject.Inject;

import static com.facebook.presto.cassandra.CassandraColumnHandle.SAMPLE_WEIGHT_COLUMN_NAME;
import static com.google.common.base.Preconditions.checkState;
import static java.nio.charset.StandardCharsets.UTF_8;
import static com.facebook.presto.cassandra.util.CassandraCqlUtils.quoteStringLiteral;

public class CassandraRecordSink implements RecordSink
{
    private final int fieldCount;
    private final int sampleWeightField;
    private final String insertQuery;
    private final CassandraSession cassandraSession;
    private final List<Object> bindedValues;
    private int field = -1;

    @Inject
    public CassandraRecordSink(CassandraOutputTableHandle handle, CassandraSession cassandraSession)
    {
        fieldCount = handle.getColumnNames().size();
        sampleWeightField = handle.getColumnNames().indexOf(SAMPLE_WEIGHT_COLUMN_NAME);
        this.cassandraSession = cassandraSession;
        StringBuilder queryBuilder = new StringBuilder(String.format("INSERT INTO \"%s\".\"%s\"(", handle.getSchemaName(), handle.getTableName()));
        queryBuilder.append("id");
        for (String columnName : handle.getColumnNames()) {
            if (!columnName.equals(SAMPLE_WEIGHT_COLUMN_NAME)) {
                queryBuilder.append(",").append(columnName);
            }
        }
        queryBuilder.append(") VALUES (?");

        for (int i = sampleWeightField > -1 ? 1 : 0; i < handle.getColumnNames().size(); i++) {
            queryBuilder.append(",?");
        }
        queryBuilder.append(")");

        insertQuery = queryBuilder.toString();
        bindedValues = Lists.newArrayList();
    }

    @Override
    public void beginRecord(long sampleWeight)
    {
        checkState(field == -1, "already in record");

        field = 0;
        bindedValues.clear();
        bindedValues.add(UUID.randomUUID());
        if (sampleWeightField == 0) {
            field++;
        }
    }

    @Override
    public void finishRecord()
    {
        checkState(field != -1, "not in record");
        checkState(field == fieldCount, "not all fields set");
        field = -1;
        cassandraSession.execute(insertQuery, bindedValues.toArray());
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
        append(quoteStringLiteral(new String(value, UTF_8)));
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
        bindedValues.add(value);
        field++;
        if (field == sampleWeightField) {
            field++;
        }
    }
}
