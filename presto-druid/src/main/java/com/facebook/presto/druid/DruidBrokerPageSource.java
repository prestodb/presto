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
package com.facebook.presto.druid;

import com.facebook.airlift.json.ObjectMapperProvider;
import com.facebook.presto.druid.DruidQueryGenerator.GeneratedDql;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.Page;
import com.facebook.presto.spi.PageBuilder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.block.BlockBuilder;
import com.facebook.presto.spi.type.BigintType;
import com.facebook.presto.spi.type.DoubleType;
import com.facebook.presto.spi.type.RealType;
import com.facebook.presto.spi.type.TimestampType;
import com.facebook.presto.spi.type.Type;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTime;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_BROKER_RESULT_ERROR;
import static com.facebook.presto.util.DateTimeZoneIndex.getChronology;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class DruidBrokerPageSource
        implements ConnectorPageSource
{
    private final ObjectMapper objecMapper = new ObjectMapperProvider().get();
    private final ConnectorSession session;
    private final GeneratedDql brokerDql;
    private final List<ColumnHandle> columnHandles;
    private final DruidClient druidClient;

    private boolean finished;
    private long readTimeNanos;
    private long completedBytes;
    private long completedPositions;

    public DruidBrokerPageSource(
            ConnectorSession session,
            GeneratedDql brokerDql,
            List<ColumnHandle> columnHandles,
            DruidClient druidClient)
    {
        this.session = requireNonNull(session, "session is null");
        this.brokerDql = requireNonNull(brokerDql, "broker is null");
        this.columnHandles = ImmutableList.copyOf(columnHandles);
        this.druidClient = requireNonNull(druidClient, "druid client is null");
    }

    @Override
    public long getCompletedBytes()
    {
        return completedBytes;
    }

    @Override
    public long getCompletedPositions()
    {
        return completedPositions;
    }

    @Override
    public long getReadTimeNanos()
    {
        return readTimeNanos;
    }

    @Override
    public boolean isFinished()
    {
        return finished;
    }

    @Override
    public Page getNextPage()
    {
        if (finished) {
            return null;
        }

        long start = System.nanoTime();
        try {
            List<DruidColumnHandle> handles = columnHandles.stream()
                    .map(column -> (DruidColumnHandle) column)
                    .collect(Collectors.toList());

            List<Type> columnTypes = handles.stream()
                    .map(DruidColumnHandle::getColumnType)
                    .collect(Collectors.toList());

            PageBuilder pageBuilder = new PageBuilder(columnTypes);

            String data = druidClient.getData(brokerDql.getDql());
            JsonNode rootNode;
            try {
                rootNode = objecMapper.readTree(data);
                checkArgument(rootNode.isArray(), "broker Druid query should return Json Array");
                ArrayNode arrayNode = (ArrayNode) rootNode;
                Iterator<JsonNode> iterator = arrayNode.elements();
                while (iterator.hasNext()) {
                    JsonNode node = iterator.next();
                    for (int i = 0; i < columnHandles.size(); i++) {
                        Type type = columnTypes.get(i);
                        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                        JsonNode value = node.get(handles.get(i).getColumnName());
                        if (type instanceof BigintType) {
                            type.writeLong(blockBuilder, value.longValue());
                        }
                        if (type instanceof DoubleType) {
                            type.writeDouble(blockBuilder, value.doubleValue());
                        }
                        if (type instanceof RealType) {
                            type.writeDouble(blockBuilder, value.doubleValue());
                        }
                        else if (type instanceof TimestampType) {
                            DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser()
                                    .withChronology(getChronology(session.getTimeZoneKey()))
                                    .withOffsetParsed()
                                    .withLocale(session.getLocale());
                            DateTime dateTime = formatter.parseDateTime(value.textValue());
                            type.writeLong(blockBuilder, dateTime.getMillis());
                        }
                        else {
                            Slice slice = Slices.utf8Slice(value.textValue());
                            blockBuilder.writeBytes(slice, 0, slice.length()).closeEntry();
                            completedBytes += slice.length();
                        }
                    }
                }
            }
            catch (IOException e) {
                throw new PrestoException(DRUID_BROKER_RESULT_ERROR, e);
            }
            pageBuilder.declarePositions(rootNode.size());
            Page page = pageBuilder.build();
            completedPositions += page.getPositionCount();
            finished = true;
            return page;
        }
        finally {
            readTimeNanos += System.nanoTime() - start;
        }
    }

    @Override
    public long getSystemMemoryUsage()
    {
        return 0;
    }

    @Override
    public void close()
    {
        finished = true;
    }
}
