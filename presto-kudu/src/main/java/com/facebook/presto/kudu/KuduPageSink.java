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
package com.facebook.presto.kudu;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.type.DecimalType;
import com.facebook.presto.common.type.SqlDate;
import com.facebook.presto.common.type.SqlDecimal;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.kudu.client.KuduException;
import org.apache.kudu.client.KuduSession;
import org.apache.kudu.client.KuduTable;
import org.apache.kudu.client.PartialRow;
import org.apache.kudu.client.SessionConfiguration;
import org.apache.kudu.client.Upsert;

import java.nio.charset.StandardCharsets;
import java.time.LocalDateTime;
import java.time.ZoneOffset;
import java.time.format.DateTimeFormatter;
import java.util.Collection;
import java.util.List;
import java.util.UUID;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.TimeUnit;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.BooleanType.BOOLEAN;
import static com.facebook.presto.common.type.DateType.DATE;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.IntegerType.INTEGER;
import static com.facebook.presto.common.type.RealType.REAL;
import static com.facebook.presto.common.type.SmallintType.SMALLINT;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TinyintType.TINYINT;
import static com.facebook.presto.common.type.VarbinaryType.VARBINARY;
import static com.facebook.presto.common.type.Varchars.isVarcharType;
import static java.lang.Float.intBitsToFloat;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class KuduPageSink
        implements ConnectorPageSink
{
    private final ConnectorSession connectorSession;
    private final KuduSession session;
    private final KuduTable table;
    private final List<Type> columnTypes;
    private final List<Type> originalColumnTypes;
    private final boolean generateUUID;

    private final String uuid;
    private int nextSubId;

    public KuduPageSink(
            ConnectorSession connectorSession,
            KuduClientSession clientSession,
            KuduInsertTableHandle tableHandle)
    {
        this(connectorSession, clientSession, tableHandle, tableHandle);
    }

    public KuduPageSink(
            ConnectorSession connectorSession,
            KuduClientSession clientSession,
            KuduOutputTableHandle tableHandle)
    {
        this(connectorSession, clientSession, tableHandle, tableHandle);
    }

    private KuduPageSink(
            ConnectorSession connectorSession,
            KuduClientSession clientSession,
            KuduTableHandle tableHandle,
            KuduTableMapping mapping)
    {
        requireNonNull(clientSession, "clientSession is null");
        this.connectorSession = connectorSession;
        this.columnTypes = mapping.getColumnTypes();
        this.originalColumnTypes = mapping.getOriginalColumnTypes();
        this.generateUUID = mapping.isGenerateUUID();

        this.table = tableHandle.getTable(clientSession);
        this.session = clientSession.newSession();
        this.session.setFlushMode(SessionConfiguration.FlushMode.AUTO_FLUSH_BACKGROUND);
        uuid = UUID.randomUUID().toString();
    }

    @Override
    public CompletableFuture<?> appendPage(Page page)
    {
        for (int position = 0; position < page.getPositionCount(); position++) {
            Upsert upsert = table.newUpsert();
            PartialRow row = upsert.getRow();
            int start = 0;
            if (generateUUID) {
                String id = String.format("%s-%08x", uuid, nextSubId++);
                row.addString(0, id);
                start = 1;
            }

            for (int channel = 0; channel < page.getChannelCount(); channel++) {
                appendColumn(row, page, position, channel, channel + start);
            }

            try {
                session.apply(upsert);
            }
            catch (KuduException e) {
                throw new RuntimeException(e);
            }
        }
        return NOT_BLOCKED;
    }

    private void appendColumn(PartialRow row, Page page, int position, int channel, int destChannel)
    {
        Block block = page.getBlock(channel);
        Type type = columnTypes.get(destChannel);
        if (block.isNull(position)) {
            row.setNull(destChannel);
        }
        else if (TIMESTAMP.equals(type)) {
            row.addLong(destChannel, type.getLong(block, position) * 1000);
        }
        else if (REAL.equals(type)) {
            row.addFloat(destChannel, intBitsToFloat((int) type.getLong(block, position)));
        }
        else if (BIGINT.equals(type)) {
            row.addLong(destChannel, type.getLong(block, position));
        }
        else if (INTEGER.equals(type)) {
            row.addInt(destChannel, (int) type.getLong(block, position));
        }
        else if (SMALLINT.equals(type)) {
            row.addShort(destChannel, (short) type.getLong(block, position));
        }
        else if (TINYINT.equals(type)) {
            row.addByte(destChannel, (byte) type.getLong(block, position));
        }
        else if (BOOLEAN.equals(type)) {
            row.addBoolean(destChannel, type.getBoolean(block, position));
        }
        else if (DOUBLE.equals(type)) {
            row.addDouble(destChannel, type.getDouble(block, position));
        }
        else if (isVarcharType(type)) {
            Type originalType = originalColumnTypes.get(destChannel);
            if (DATE.equals(originalType)) {
                SqlDate date = (SqlDate) originalType.getObjectValue(connectorSession.getSqlFunctionProperties(), block, position);
                LocalDateTime ldt = LocalDateTime.ofEpochSecond(TimeUnit.DAYS.toSeconds(date.getDays()), 0, ZoneOffset.UTC);
                byte[] bytes = ldt.format(DateTimeFormatter.ISO_LOCAL_DATE).getBytes(StandardCharsets.UTF_8);
                row.addStringUtf8(destChannel, bytes);
            }
            else {
                row.addString(destChannel, type.getSlice(block, position).toStringUtf8());
            }
        }
        else if (VARBINARY.equals(type)) {
            row.addBinary(destChannel, type.getSlice(block, position).toByteBuffer());
        }
        else if (type instanceof DecimalType) {
            SqlDecimal sqlDecimal = (SqlDecimal) type.getObjectValue(connectorSession.getSqlFunctionProperties(), block, position);
            row.addDecimal(destChannel, sqlDecimal.toBigDecimal());
        }
        else {
            throw new UnsupportedOperationException("Type is not supported: " + type);
        }
    }

    @Override
    public CompletableFuture<Collection<Slice>> finish()
    {
        closeSession();
        return completedFuture(ImmutableList.of());
    }

    @Override
    public void abort()
    {
        closeSession();
    }

    private void closeSession()
    {
        try {
            session.close();
        }
        catch (KuduException e) {
            throw new RuntimeException(e);
        }
    }
}
