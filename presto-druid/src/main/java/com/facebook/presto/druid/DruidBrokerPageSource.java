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

import com.facebook.airlift.json.JsonObjectMapperProvider;
import com.facebook.presto.common.Page;
import com.facebook.presto.common.PageBuilder;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.BigintType;
import com.facebook.presto.common.type.DoubleType;
import com.facebook.presto.common.type.RealType;
import com.facebook.presto.common.type.TimestampType;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.druid.DruidQueryGenerator.GeneratedDql;
import com.facebook.presto.spi.ColumnHandle;
import com.facebook.presto.spi.ConnectorPageSource;
import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;
import org.joda.time.DateTime;
import org.joda.time.chrono.ISOChronology;
import org.joda.time.format.DateTimeFormatter;
import org.joda.time.format.ISODateTimeFormat;

import java.io.BufferedReader;
import java.io.IOException;
import java.io.InputStreamReader;
import java.util.List;

import static com.facebook.presto.druid.DruidErrorCode.DRUID_BROKER_RESULT_ERROR;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.lang.Float.floatToRawIntBits;
import static java.util.Objects.requireNonNull;

public class DruidBrokerPageSource
        implements ConnectorPageSource
{
    private static final ObjectMapper OBJECT_MAPPER = new JsonObjectMapperProvider().get();

    private final List<ColumnHandle> columnHandles;

    private boolean finished;
    private long readTimeNanos;
    private long completedBytes;
    private long completedPositions;
    private BufferedReader responseStream;
    private final PageBuilder pageBuilder;
    private List<Type> columnTypes;

    public DruidBrokerPageSource(
            GeneratedDql brokerDql,
            List<ColumnHandle> columnHandles,
            DruidClient druidClient)
    {
        requireNonNull(brokerDql, "broker is null");
        this.columnHandles = ImmutableList.copyOf(requireNonNull(columnHandles, "columnHandles is null"));
        requireNonNull(druidClient, "druid client is null");
        this.responseStream = new BufferedReader(new InputStreamReader(druidClient.getData(brokerDql.getDql())));
        List<DruidColumnHandle> handles = columnHandles.stream()
                .map(column -> (DruidColumnHandle) column)
                .collect(toImmutableList());
        this.columnTypes = handles.stream()
                .map(DruidColumnHandle::getColumnType)
                .collect(toImmutableList());
        this.pageBuilder = new PageBuilder(this.columnTypes);
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
        boolean columnHandlesHasErrorMessageField = columnHandles.stream().anyMatch(
                handle -> ((DruidColumnHandle) handle).getColumnName().equals("errorMessage"));
        try {
            String readLine;
            while ((readLine = responseStream.readLine()) != null) {
                // if read a blank line,it means read finish
                if (readLine.isEmpty()) {
                    finished = true;
                    break;
                }
                else {
                    JsonNode rootNode = OBJECT_MAPPER.readTree(readLine);
                    if (rootNode.has("errorMessage") && !columnHandlesHasErrorMessageField) {
                        throw new PrestoException(DRUID_BROKER_RESULT_ERROR, rootNode.findValue("errorMessage").asText());
                    }
                    for (int i = 0; i < columnHandles.size(); i++) {
                        Type type = columnTypes.get(i);
                        BlockBuilder blockBuilder = pageBuilder.getBlockBuilder(i);
                        JsonNode value = rootNode.get(((DruidColumnHandle) columnHandles.get(i)).getColumnName());
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
                            type.writeLong(blockBuilder, floatToRawIntBits(value.floatValue()));
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
                            type.writeSlice(blockBuilder, slice);
                        }
                    }
                }
                pageBuilder.declarePosition();
                if (pageBuilder.isFull()) {
                    break;
                }
            }
            // if responseStream.readLine() is null, it means read finish
            if (readLine == null) {
                finished = true;
                return null;
            }

            // only return a page if the buffer is full or we are finishing
            if (pageBuilder.isEmpty() || (!finished && !pageBuilder.isFull())) {
                return null;
            }

            Page page = pageBuilder.build();
            completedPositions += page.getPositionCount();
            completedBytes += page.getSizeInBytes();
            pageBuilder.reset();
            return page;
        }
        catch (IOException e) {
            finished = true;
            throw new PrestoException(DRUID_BROKER_RESULT_ERROR, "Parse druid client response error", e);
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
