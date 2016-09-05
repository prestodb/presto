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
package com.facebook.presto.hbase;

import static com.facebook.presto.hbase.HBaseErrorCode.HBASE_SPLIT_ERROR;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.Iterator;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.client.Scan;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.facebook.presto.spi.type.Type;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableSet;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

/**
 * HBase specific record set. Returns a cursor for a topic which iterates over a Kafka partition segment.
 */
public class HBaseRecordSet
        implements RecordSet
{
    private static final Logger log = Logger.get(HBaseRecordSet.class);

    private static final int HBASE_READ_BUFFER_SIZE = 100_000;
    private static final byte [] EMPTY_BYTE_ARRAY = new byte [0];

    private final HBaseSplit split;
    private final HBaseSimpleConsumerManager consumerManager;

    private final RowDecoder keyDecoder;
    private final RowDecoder messageDecoder;
    private final Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders;
    private final Map<DecoderColumnHandle, FieldDecoder<?>> messageFieldDecoders;

    private final List<DecoderColumnHandle> columnHandles;
    private final List<Type> columnTypes;

    private final Set<FieldValueProvider> globalInternalFieldValueProviders;

    HBaseRecordSet(HBaseSplit split,
            HBaseSimpleConsumerManager consumerManager,
            List<DecoderColumnHandle> columnHandles,
            RowDecoder keyDecoder,
            RowDecoder messageDecoder,
            Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders,
            Map<DecoderColumnHandle, FieldDecoder<?>> messageFieldDecoders)
    {
        this.split = requireNonNull(split, "split is null");

        this.globalInternalFieldValueProviders = ImmutableSet.of(
                HBaseInternalFieldDescription.PARTITION_ID_FIELD.forLongValue(split.getPartitionId()),
                HBaseInternalFieldDescription.SEGMENT_START_FIELD.forLongValue(0),
                HBaseInternalFieldDescription.SEGMENT_END_FIELD.forLongValue(0));

        this.consumerManager = requireNonNull(consumerManager, "consumerManager is null");

        this.keyDecoder = requireNonNull(keyDecoder, "rowDecoder is null");
        this.messageDecoder = requireNonNull(messageDecoder, "rowDecoder is null");
        this.keyFieldDecoders = requireNonNull(keyFieldDecoders, "keyFieldDecoders is null");
        this.messageFieldDecoders = requireNonNull(messageFieldDecoders, "messageFieldDecoders is null");

        this.columnHandles = requireNonNull(columnHandles, "columnHandles is null");

        ImmutableList.Builder<Type> typeBuilder = ImmutableList.builder();

        for (DecoderColumnHandle handle : columnHandles) {
            typeBuilder.add(handle.getType());
        }

        this.columnTypes = typeBuilder.build();
    }

    @Override
    public List<Type> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        return new HBaseRecordCursor();
    }

    public class HBaseRecordCursor
            implements RecordCursor
    {
        private long totalBytes;
        private long totalMessages;
        private byte[] cursorOffset = split.getStart();
        //private Iterator<MessageAndOffset> messageAndOffsetIterator;
        private Iterator<Result> messageAndOffsetIterator;
        private final AtomicBoolean reported = new AtomicBoolean();

        private FieldValueProvider[] fieldValueProviders;

        HBaseRecordCursor()
        {
        }

        @Override
        public long getTotalBytes()
        {
            return totalBytes;
        }

        @Override
        public long getCompletedBytes()
        {
            return totalBytes;
        }

        @Override
        public long getReadTimeNanos()
        {
            return 0;
        }

        @Override
        public Type getType(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");
            return columnHandles.get(field).getType();
        }

        @Override
        public boolean advanceNextPosition()
        {
            while (true) {
                // Create a fetch request
                openFetchRequest();

                while (messageAndOffsetIterator.hasNext()) {
                	Result currentMessageAndOffset = messageAndOffsetIterator.next();
                    return nextRow(currentMessageAndOffset);
                }
                messageAndOffsetIterator = null;
                return endOfData(); // Past our split end. Bail.
            }
        }

        private boolean endOfData()
        {
            if (!reported.getAndSet(true)) {
                log.debug("Found a total of %d messages with %d bytes", totalMessages, totalBytes);
            }
            return false;
        }

        private boolean nextRow(Result messageAndOffset)
        {
            cursorOffset = messageAndOffset.getRow(); // Cursor now points to the next message.
            totalBytes += Result.getTotalSizeOfCells(messageAndOffset);
            totalMessages++;

            //byte[] keyData = EMPTY_BYTE_ARRAY;
            //byte[] messageData = EMPTY_BYTE_ARRAY;
            //ByteBuffer key = messageAndOffset.message().key();
            //if (key != null) {
            //    keyData = new byte[key.remaining()];
            //    key.get(keyData);
            //}
 
            //ByteBuffer message = messageAndOffset.message().payload();
            //if (message != null) {
            //    messageData = new byte[message.remaining()];
            //    message.get(messageData);
            //}
            
            StringBuilder sb = new StringBuilder();
            sb.append("{");
            boolean first = true;
            for (Cell cell : messageAndOffset.listCells()) {
            	if (first) {
            		first = false;
            	} else {
            		sb.append(",");
            	}
            	sb.append("\"");
            	sb.append(Bytes.toString(CellUtil.cloneFamily(cell)));
            	sb.append(":");
            	sb.append(Bytes.toString(CellUtil.cloneQualifier(cell)));
            	sb.append("\":\"");
            	sb.append(Bytes.toString(CellUtil.cloneValue(cell)));
            	sb.append("\"");
            }
            sb.append("}");
            
            byte[] messageData = EMPTY_BYTE_ARRAY;
            ByteBuffer message = ByteBuffer.wrap(sb.toString().getBytes());
            if (message != null) {
                messageData = new byte[message.remaining()];
                message.get(messageData);
            }

            Set<FieldValueProvider> fieldValueProviders = new HashSet<>();

            fieldValueProviders.addAll(globalInternalFieldValueProviders);
            fieldValueProviders.add(HBaseInternalFieldDescription.SEGMENT_COUNT_FIELD.forLongValue(totalMessages));
            fieldValueProviders.add(HBaseInternalFieldDescription.PARTITION_OFFSET_FIELD.forLongValue(0 /*messageAndOffset.offset()*/));
            fieldValueProviders.add(HBaseInternalFieldDescription.MESSAGE_FIELD.forByteValue(messageData));
            fieldValueProviders.add(HBaseInternalFieldDescription.MESSAGE_LENGTH_FIELD.forLongValue(messageData.length));
            fieldValueProviders.add(HBaseInternalFieldDescription.KEY_FIELD.forByteValue(messageAndOffset.getRow()));
            fieldValueProviders.add(HBaseInternalFieldDescription.KEY_LENGTH_FIELD.forLongValue(messageAndOffset.getRow().length));
            fieldValueProviders.add(HBaseInternalFieldDescription.KEY_CORRUPT_FIELD.forBooleanValue(false));
            fieldValueProviders.add(HBaseInternalFieldDescription.MESSAGE_CORRUPT_FIELD.forBooleanValue(messageDecoder.decodeRow(messageData, null, fieldValueProviders, columnHandles, messageFieldDecoders)));

            this.fieldValueProviders = new FieldValueProvider[columnHandles.size()];

            // If a value provider for a requested internal column is present, assign the
            // value to the internal cache. It is possible that an internal column is present
            // where no value provider exists (e.g. the '_corrupt' column with the DummyRowDecoder).
            // In that case, the cache is null (and the column is reported as null).
            for (int i = 0; i < columnHandles.size(); i++) {
                for (FieldValueProvider fieldValueProvider : fieldValueProviders) {
                    if (fieldValueProvider.accept(columnHandles.get(i))) {
                        this.fieldValueProviders[i] = fieldValueProvider;
                        break; // for(InternalColumnProvider...
                    }
                }
            }

            return true; // Advanced successfully.
        }

        @Override
        public boolean getBoolean(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, boolean.class);
            return isNull(field) ? false : fieldValueProviders[field].getBoolean();
        }

        @Override
        public long getLong(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, long.class);
            return isNull(field) ? 0L : fieldValueProviders[field].getLong();
        }

        @Override
        public double getDouble(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, double.class);
            return isNull(field) ? 0.0d : fieldValueProviders[field].getDouble();
        }

        @Override
        public Slice getSlice(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            checkFieldType(field, Slice.class);
            return isNull(field) ? Slices.EMPTY_SLICE : fieldValueProviders[field].getSlice();
        }

        @Override
        public Object getObject(int field)
        {
            throw new UnsupportedOperationException();
        }

        @Override
        public boolean isNull(int field)
        {
            checkArgument(field < columnHandles.size(), "Invalid field index");

            return fieldValueProviders[field] == null || fieldValueProviders[field].isNull();
        }

        private void checkFieldType(int field, Class<?> expected)
        {
            Class<?> actual = getType(field).getJavaType();
            checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
        }

        @Override
        public void close()
        {
        }

        private void openFetchRequest()
        {
            if (messageAndOffsetIterator == null) {
            	log.debug("Fetching from HBase");
                //log.debug("Fetching %d bytes from offset %d (%d - %d). %d messages read so far", HBASE_READ_BUFFER_SIZE, cursorOffset, split.getStart(), split.getEnd(), totalMessages);
                /*FetchRequest req = new FetchRequestBuilder()
                        .clientId("presto-worker-" + Thread.currentThread().getName())
                        .addFetch(split.getTopicName(), split.getPartitionId(), cursorOffset, HBASE_READ_BUFFER_SIZE)
                        .build();*/
                Scan scan = new Scan();
                scan.setStartRow(split.getStart());
                scan.setStopRow(split.getEnd());

                Connection consumer = consumerManager.getConsumer(split.getLeader());

                try 
                {
                	Table table = consumer.getTable(TableName.valueOf(split.getTopicName()));
                
	                ResultScanner scanner = table.getScanner(scan);

	                messageAndOffsetIterator = scanner.iterator();
                } catch (IOException ex) {
                	log.warn("Fetch response has error: %d", ex.getMessage());
                    throw new PrestoException(HBASE_SPLIT_ERROR, "could not get scanner from HBase, error is '" + ex.getMessage() + "'");
                }
            }
        }

    }
}
