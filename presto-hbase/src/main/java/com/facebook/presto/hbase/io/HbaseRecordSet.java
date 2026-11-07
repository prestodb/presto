package com.facebook.presto.hbase.io;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.hbase.HbaseClient;
import com.facebook.presto.hbase.model.HbaseColumnConstraint;
import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.hbase.model.HbaseSplit;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hbase.client.Result;
import org.apache.hadoop.hbase.io.ImmutableBytesWritable;
import org.apache.hadoop.mapreduce.RecordReader;

import java.util.List;

import static com.facebook.presto.hbase.HbaseErrorCode.UNEXPECTED_HBASE_ERROR;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class HbaseRecordSet implements RecordSet {
  private final List<HbaseColumnHandle> columnHandles;
  private final List<HbaseColumnConstraint> constraints;
  private final List<Type> columnTypes;

  private final RecordReader<ImmutableBytesWritable, Result> resultRecordReader;
  private final String rowIdName;

  public HbaseRecordSet(HbaseClient hbaseClient, ConnectorSession session, HbaseSplit split,
      List<HbaseColumnHandle> columnHandles) {
    requireNonNull(session, "session is null");
    requireNonNull(split, "split is null");
    constraints = requireNonNull(split.getConstraints(), "constraints is null");

    rowIdName = split.getRowId();

    // Save off the column handles and createa list of the Hbase types
    this.columnHandles = requireNonNull(columnHandles, "column handles is null");
    ImmutableList.Builder<Type> types = ImmutableList.builder();
    for (HbaseColumnHandle column : columnHandles) {
      types.add(column.getType());
    }
    this.columnTypes = types.build();

    // Create the BatchScanner and set the ranges from the split

    try {
      RecordReader<ImmutableBytesWritable, Result> resultRecordReader =
          hbaseClient.execSplit(session, split, columnHandles);
      this.resultRecordReader = resultRecordReader;
    } catch (Exception e) {
      throw new PrestoException(UNEXPECTED_HBASE_ERROR,
          format("Failed to create batch scan for table %s", split.getFullTableName()), e);
    }
  }

  @Override
  public List<Type> getColumnTypes() {
    return columnTypes;
  }

  @Override
  public RecordCursor cursor() {
    return new HbaseRecordCursor(resultRecordReader, rowIdName, columnHandles, constraints);
  }
}
