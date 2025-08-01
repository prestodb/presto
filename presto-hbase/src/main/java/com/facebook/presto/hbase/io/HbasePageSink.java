package com.facebook.presto.hbase.io;

import com.facebook.presto.common.Page;
import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeUtils;
import com.facebook.presto.hbase.metadata.HbaseTable;
import com.facebook.presto.hbase.model.HbaseColumnHandle;
import com.facebook.presto.spi.ConnectorPageSink;
import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hadoop.hbase.TableName;
import org.apache.hadoop.hbase.TableNotFoundException;
import org.apache.hadoop.hbase.client.Connection;
import org.apache.hadoop.hbase.client.Put;
import org.apache.hadoop.hbase.client.Table;
import org.apache.hadoop.hbase.util.Bytes;
import org.apache.hadoop.io.Text;

import java.io.IOException;
import java.util.ArrayList;
import java.util.Collection;
import java.util.List;
import java.util.Optional;
import java.util.concurrent.CompletableFuture;

import static com.facebook.presto.hbase.HbaseErrorCode.HBASE_TABLE_DNE;
import static com.facebook.presto.hbase.HbaseErrorCode.UNEXPECTED_HBASE_ERROR;
import static com.facebook.presto.hbase.util.HbaseRowSerializerUtil.toHbaseBytes;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_IMPLEMENTATION_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static io.airlift.concurrent.MoreFutures.getFutureValue;
import static java.util.Objects.requireNonNull;
import static java.util.concurrent.CompletableFuture.completedFuture;

public class HbasePageSink implements ConnectorPageSink {
  public static final Text ROW_ID_COLUMN = new Text("___ROW___");
  public static final int MAX_PUT_NUM = 10_000;

  private final List<Put> puts;
  private Table hTable;
  private long numRows;
  private final List<HbaseColumnHandle> columns;
  private final int rowIdOrdinal;

  public HbasePageSink(Connection connection, HbaseTable table) {
    requireNonNull(table, "table is null");
    this.columns = table.getColumns();

    // Fetch the row ID ordinal, throwing an exception if not found for safety
    Optional<Integer> ordinal =
        columns.stream().filter(columnHandle -> columnHandle.getName().equals(table.getRowId()))
            .map(HbaseColumnHandle::getOrdinal).findAny();

    if (!ordinal.isPresent()) {
      throw new PrestoException(FUNCTION_IMPLEMENTATION_ERROR, "Row ID ordinal not found");
    }
    this.rowIdOrdinal = ordinal.get();
    this.puts = new ArrayList<>(MAX_PUT_NUM);
    try {
      this.hTable = connection.getTable(TableName.valueOf(table.getSchema(), table.getTable()));
    } catch (TableNotFoundException e) {
      throw new PrestoException(HBASE_TABLE_DNE,
          "Hbase error when getting htable and/or Indexer, table does not exist", e);
    } catch (IOException e) {
      throw new PrestoException(UNEXPECTED_HBASE_ERROR,
          "Hbase error when getting htable and/or Indexer", e);
    }
  }

  @Override
  public CompletableFuture<?> appendPage(Page page) {
    // For each position within the page, i.e. row
    for (int position = 0; position < page.getPositionCount(); ++position) {
      Type rowkeyType = columns.get(rowIdOrdinal).getType();
      Object rowKey = TypeUtils.readNativeValue(rowkeyType, page.getBlock(rowIdOrdinal), position);
      if (rowKey == null) {
        throw new PrestoException(INVALID_FUNCTION_ARGUMENT,
            "Column mapped as the Hbase row ID cannot be null");
      }
      Put put = new Put(toHbaseBytes(rowkeyType, rowKey));

      // For each channel within the page, i.e. column
      for (HbaseColumnHandle column : columns) {
        // Skip the row ID ordinal
        if (column.getOrdinal() == rowIdOrdinal) {
          continue;
        }
        // Get the type for this channel
        int channel = column.getOrdinal();

        // Read the value from the page and append the field to the row
        Object value =
            TypeUtils.readNativeValue(column.getType(), page.getBlock(channel), position);
        put.addColumn(Bytes.toBytes(column.getFamily().get()),
            Bytes.toBytes(column.getQualifier().get()), toHbaseBytes(column.getType(), value));
      }

      // Convert row to a Mutation, writing and indexing it
      puts.add(put);
      ++numRows;

      if (numRows % MAX_PUT_NUM == 0) {
        flush();
      }
    }

    return NOT_BLOCKED;
  }

  private void flush() {
    try {
      hTable.put(puts);
    } catch (IOException e) {
      throw new PrestoException(UNEXPECTED_HBASE_ERROR, "puts rejected by server on flush", e);
    } finally {
      puts.clear();
      numRows = 0;
    }
  }

  @Override
  public CompletableFuture<Collection<Slice>> finish() {
    try (Table table = hTable) {
      flush();
    } catch (IOException e) {
      throw new PrestoException(UNEXPECTED_HBASE_ERROR, "Error when htable closes", e);
    }
    return completedFuture(ImmutableList.of());
  }

  @Override
  public void abort() {
    getFutureValue(finish());
  }
}
