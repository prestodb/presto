package com.facebook.presto.hbase;

import static com.facebook.presto.hbase.HBaseErrorCode.HBASE_CURSOR_ERROR;
import static com.google.common.base.Preconditions.checkArgument;

import java.io.IOException;
import java.nio.ByteBuffer;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;
import java.util.concurrent.atomic.AtomicBoolean;

import org.apache.hadoop.hbase.Cell;
import org.apache.hadoop.hbase.CellUtil;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.client.ResultScanner;
import org.apache.hadoop.hbase.util.Bytes;

import com.facebook.presto.decoder.DecoderColumnHandle;
import com.facebook.presto.decoder.FieldDecoder;
import com.facebook.presto.decoder.FieldValueProvider;
import com.facebook.presto.decoder.RowDecoder;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.type.Type;

import io.airlift.log.Logger;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

public class HBaseRecordCursor implements RecordCursor {

	private static final Logger log = Logger.get(HBaseRecordCursor.class);

    //private static final int HBASE_READ_BUFFER_SIZE = 100_000;
    private static final byte [] EMPTY_BYTE_ARRAY = new byte [0];
			
	private long totalBytes;
	private long totalMessages;

	// private Iterator<MessageAndOffset> messageAndOffsetIterator;
	//private Result messageAndOffsetIterator;
	private final AtomicBoolean reported = new AtomicBoolean();

	private FieldValueProvider[] fieldValueProviders;
	private List<DecoderColumnHandle> columnHandles;

	private Set<FieldValueProvider> globalInternalFieldValueProviders;

	private ResultScanner scanner;

	private RowDecoder keyDecoder;

	private RowDecoder messageDecoder;

	private Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders;

	private Map<DecoderColumnHandle, FieldDecoder<?>> messageFieldDecoders;

	private static HBaseSplit split;

	HBaseRecordCursor(HBaseSplit split, Set<FieldValueProvider> globalInternalFieldValueProviders, List<DecoderColumnHandle> columnHandles, ResultScanner scanner, 
			RowDecoder keyDecoder, RowDecoder messageDecoder, Map<DecoderColumnHandle, FieldDecoder<?>> keyFieldDecoders, 
			Map<DecoderColumnHandle, FieldDecoder<?>> messageFieldDecoders) {
		this.split = split;
		this.globalInternalFieldValueProviders = globalInternalFieldValueProviders;
		this.columnHandles = columnHandles;
		this.scanner = scanner;
		this.keyDecoder = keyDecoder;
		this.messageDecoder = messageDecoder;
		this.keyFieldDecoders = keyFieldDecoders;
		this.messageFieldDecoders = messageFieldDecoders;
	}

	@Override
	public long getTotalBytes() {
		return totalBytes;
	}

	@Override
	public long getCompletedBytes() {
		return totalBytes;
	}

	@Override
	public long getReadTimeNanos() {
		return 0;
	}

	@Override
	public Type getType(int field) {
		checkArgument(field < columnHandles.size(), "Invalid field index");
		return columnHandles.get(field).getType();
	}

	@Override
	public boolean advanceNextPosition() {
		try
		{
			Result currentMessageAndOffset = scanner.next();
			if (currentMessageAndOffset != null) {
				nextRow(currentMessageAndOffset);
				return true;
			} else {
				return endOfData(); // Past our split end. Bail.
			}
		} catch (IOException ex) {
			throw new PrestoException(HBASE_CURSOR_ERROR, "could not get next from HBase scanner, error is '" + ex.getMessage() + "'");
		}
	}

	private boolean endOfData() {
		if (!reported.getAndSet(true)) {
			log.debug("Found a total of %d messages with %d bytes", totalMessages, totalBytes);
		}
		return false;
	}

	private boolean nextRow(Result messageAndOffset) {
		totalBytes += Result.getTotalSizeOfCells(messageAndOffset);
		totalMessages++;

		byte[] keyData = messageAndOffset.getRow();
		// byte[] messageData = EMPTY_BYTE_ARRAY;
		// ByteBuffer key = messageAndOffset.message().key();
		// if (key != null) {
		// keyData = new byte[key.remaining()];
		// key.get(keyData);
		// }

		// ByteBuffer message = messageAndOffset.message().payload();
		// if (message != null) {
		// messageData = new byte[message.remaining()];
		// message.get(messageData);
		// }

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

		byte[] messageData = sb.toString().getBytes(); //EMPTY_BYTE_ARRAY;
		ByteBuffer message = ByteBuffer.wrap(messageData);
		if (message != null) {
			messageData = new byte[message.remaining()];
			message.get(messageData);
		}

		Set<FieldValueProvider> fieldValueProviders = new HashSet<>();

		fieldValueProviders.addAll(globalInternalFieldValueProviders);
		fieldValueProviders.add(HBaseInternalFieldDescription.SEGMENT_COUNT_FIELD.forLongValue(totalMessages));
		fieldValueProviders.add(HBaseInternalFieldDescription.PARTITION_OFFSET_FIELD
				.forLongValue(0 /* messageAndOffset.offset() */));
		fieldValueProviders.add(HBaseInternalFieldDescription.MESSAGE_FIELD.forByteValue(messageData));
		fieldValueProviders.add(HBaseInternalFieldDescription.MESSAGE_LENGTH_FIELD.forLongValue(messageData.length));
		fieldValueProviders.add(HBaseInternalFieldDescription.KEY_FIELD.forByteValue(messageAndOffset.getRow()));
		fieldValueProviders
				.add(HBaseInternalFieldDescription.KEY_LENGTH_FIELD.forLongValue(messageAndOffset.getRow().length));

		fieldValueProviders.add(HBaseInternalFieldDescription.KEY_CORRUPT_FIELD.forBooleanValue(true));
        fieldValueProviders.add(HBaseInternalFieldDescription.MESSAGE_CORRUPT_FIELD.forBooleanValue(true));

        //fieldValueProviders.add(HBaseInternalFieldDescription.KEY_CORRUPT_FIELD.forBooleanValue(keyDecoder.decodeRow(keyData, null, fieldValueProviders, columnHandles, keyFieldDecoders)));
        //fieldValueProviders.add(HBaseInternalFieldDescription.MESSAGE_CORRUPT_FIELD.forBooleanValue(messageDecoder.decodeRow(messageData, null, fieldValueProviders, columnHandles, messageFieldDecoders)));

		this.fieldValueProviders = new FieldValueProvider[columnHandles.size()];

		// If a value provider for a requested internal column is present,
		// assign the
		// value to the internal cache. It is possible that an internal column
		// is present
		// where no value provider exists (e.g. the '_corrupt' column with the
		// DummyRowDecoder).
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
	public boolean getBoolean(int field) {
		checkArgument(field < columnHandles.size(), "Invalid field index");

		checkFieldType(field, boolean.class);
		return isNull(field) ? false : fieldValueProviders[field].getBoolean();
	}

	@Override
	public long getLong(int field) {
		checkArgument(field < columnHandles.size(), "Invalid field index");

		checkFieldType(field, long.class);
		return isNull(field) ? 0L : fieldValueProviders[field].getLong();
	}

	@Override
	public double getDouble(int field) {
		checkArgument(field < columnHandles.size(), "Invalid field index");

		checkFieldType(field, double.class);
		return isNull(field) ? 0.0d : fieldValueProviders[field].getDouble();
	}

	@Override
	public Slice getSlice(int field) {
		checkArgument(field < columnHandles.size(), "Invalid field index");

		checkFieldType(field, Slice.class);
		return isNull(field) ? Slices.EMPTY_SLICE : fieldValueProviders[field].getSlice();
	}

	@Override
	public Object getObject(int field) {
		throw new UnsupportedOperationException();
	}

	@Override
	public boolean isNull(int field) {
		checkArgument(field < columnHandles.size(), "Invalid field index");

		return fieldValueProviders[field] == null || fieldValueProviders[field].isNull();
	}

	private void checkFieldType(int field, Class<?> expected) {
		Class<?> actual = getType(field).getJavaType();
		checkArgument(actual == expected, "Expected field %s to be type %s but is %s", field, expected, actual);
	}

	@Override
	public void close() {
	}

}