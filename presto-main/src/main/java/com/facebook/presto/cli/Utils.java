package com.facebook.presto.cli;

import com.facebook.presto.block.BlockIterable;
import com.facebook.presto.metadata.ColumnMetadata;
import com.facebook.presto.metadata.Metadata;
import com.facebook.presto.metadata.StorageManager;
import com.facebook.presto.metadata.TableMetadata;
import com.facebook.presto.operator.ConsolePrinter;
import com.facebook.presto.operator.Operator;
import io.airlift.units.Duration;

public class Utils
{
    public static BlockIterable getColumn(StorageManager storageManager, Metadata metadata, final String tableName, String columnName)
    {
        TableMetadata tableMetadata = metadata.getTable("default", "default", tableName);
        int index = 0;
        for (ColumnMetadata columnMetadata : tableMetadata.getColumns()) {
            if (columnName.equals(columnMetadata.getName())) {
                break;
            }
            ++index;
        }
        final int columnIndex = index;
        return storageManager.getBlocks("default", tableName, columnIndex);
    }

    public static void printResults(long start, Operator aggregation)
    {
        long rows = ConsolePrinter.print(aggregation);
        Duration duration = Duration.nanosSince(start);
        System.out.printf("%d rows in %4.2f ms\n", rows, duration.toMillis());
    }
}
