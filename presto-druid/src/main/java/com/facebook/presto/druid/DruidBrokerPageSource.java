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
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.PrestoException;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.druid.DruidQueryGenerator.GeneratedDql;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.fasterxml.jackson.databind.node.ArrayNode;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.IOException;
import java.util.Iterator;
import java.util.List;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_BROKER_RESULT_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class DruidBrokerPageSource
        implements ConnectorPageSource
{
    private static final ObjectMapper OBJECT_MAPPER = new ObjectMapperProvider().get();

    private final GeneratedDql brokerDql;
    private final List<ColumnHandle> columnHandles;
    private final DruidClient druidClient;

    private boolean finished;
    private long readTimeNanos;
    private long completedBytes;
    private long completedPositions;

    public DruidBrokerPageSource(
            GeneratedDql brokerDql,
            List<ColumnHandle> columnHandles,
            DruidClient druidClient)
    {
        this.brokerDql = requireNonNull(brokerDql, "broker is null");
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
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
                    .collect(toImmutableList());

            List<Type> columnTypes = handles.stream()
                    .map(DruidColumnHandle::getColumnType)
                    .collect(toImmutableList());

            PageBuilder pageBuilder = new PageBuilder(columnTypes);

            String data = druidClient.getData(brokerDql.getDql());
            JsonNode rootNode;
            try {
                rootNode = OBJECT_MAPPER.readTree(data);
                checkArgument(rootNode.isArray(), "broker Druid query should return Json Array: " + data);
                ArrayNode arrayNode = (ArrayNode) rootNode;
                Iterator<JsonNode> iterator = arrayNode.elements();
                while (iterator.hasNext()) {
                    JsonNode node = iterator.next();
                    Iterator<String> fieldNamesIterator = node.fieldNames();
                    for (int i = 0; i < columnHandles.size(); i++) {
                        Type type = columnTypes.get(i);
                        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);

                        String fieldName = fieldNamesIterator.next();
                        JsonNode value = node.get(fieldName);
                        if (value == null) {
                            blockBuilder.appendNull();
                            continue;
                        }
                        if (type instanceof BigintType) {
                            type.writeLong(blockBuilder, value.longValue());
                        }
                        else if (type instanceof DoubleType) {
                            type.writeDouble(blockBuilder, value.doubleValue());
                        }
                        else if (type instanceof RealType) {
                            type.writeLong(blockBuilder, value.longValue());
                        }
                        else if (type instanceof TimestampType) {
                            DateTimeFormatter formatter = ISODateTimeFormat.dateTimeParser()
                                    .withChronology(ISOChronology.getInstanceUTC())
                                    .withOffsetParsed();
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
