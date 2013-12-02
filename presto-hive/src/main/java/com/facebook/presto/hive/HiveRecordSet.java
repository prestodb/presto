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
package com.facebook.presto.hive;

import com.facebook.presto.hadoop.HadoopNative;
import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.base.Optional;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.SerDeException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.Properties;
import java.util.concurrent.Callable;

import static com.facebook.presto.hive.HiveColumnHandle.hiveColumnIndexGetter;
import static com.facebook.presto.hive.HiveColumnHandle.isPartitionKeyPredicate;
import static com.facebook.presto.hive.HiveColumnHandle.nativeTypeGetter;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.facebook.presto.hive.HiveUtil.getInputFormatName;
import static com.facebook.presto.hive.HiveUtil.getTableObjectInspector;
import static com.facebook.presto.hive.RetryDriver.retry;
import static com.google.common.base.Preconditions.checkNotNull;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.base.Predicates.not;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.transform;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_NULL_FORMAT;

public class HiveRecordSet
        implements RecordSet
{
    static {
        HadoopNative.requireHadoopNative();
    }

    private final HiveSplit split;
    private final List<HiveColumnHandle> columns;
    private final List<ColumnType> columnTypes;
    private final List<Integer> readHiveColumnIndexes;
    private final Configuration configuration;
    private final Path wrappedPath;
    private final List<HiveRecordCursorProvider> cursorProviders;

    public HiveRecordSet(HdfsEnvironment hdfsEnvironment, HiveSplit split, List<HiveColumnHandle> columns, List<HiveRecordCursorProvider> cursorProviders)
    {
        this.split = checkNotNull(split, "split is null");
        this.columns = ImmutableList.copyOf(checkNotNull(columns, "columns is null"));
        this.columnTypes = ImmutableList.copyOf(Iterables.transform(columns, nativeTypeGetter()));
        this.cursorProviders = ImmutableList.copyOf(checkNotNull(cursorProviders, "cursor providers is null"));

        // determine which hive columns we will read
        List<HiveColumnHandle> readColumns = ImmutableList.copyOf(filter(columns, not(isPartitionKeyPredicate())));
        if (readColumns.isEmpty()) {
            // for count(*) queries we will have "no" columns we want to read, but since hive doesn't
            // support no columns (it will read all columns instead), we must choose a single column
            HiveColumnHandle primitiveColumn = getFirstPrimitiveColumn(split.getClientId(), split.getSchema());
            readColumns = ImmutableList.of(primitiveColumn);
        }
        readHiveColumnIndexes = new ArrayList<>(transform(readColumns, hiveColumnIndexGetter()));

        Path path = new Path(split.getPath());
        this.configuration = hdfsEnvironment.getConfiguration(path);
        this.wrappedPath = hdfsEnvironment.getFileSystemWrapper().wrap(path);

        String nullSequence = split.getSchema().getProperty(SERIALIZATION_NULL_FORMAT);
        checkState(nullSequence == null || nullSequence.equals("\\N"), "Only '\\N' supported as null specifier, was '%s'", nullSequence);
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        // Tell hive the columns we would like to read, this lets hive optimize reading column oriented files
        ColumnProjectionUtils.setReadColumnIDs(configuration, readHiveColumnIndexes);

        RecordReader<?, ?> recordReader = createRecordReader(split, configuration, wrappedPath);

        for (HiveRecordCursorProvider provider: cursorProviders) {
            Optional<RecordCursor> cursor = provider.createHiveRecordCursor(split, recordReader, columns);
            if (cursor.isPresent()) {
                return cursor.get();
            }
        }

        throw new RuntimeException("Configured cursor providers did not provide a cursor");
    }

    private static HiveColumnHandle getFirstPrimitiveColumn(String clientId, Properties schema)
    {
        try {
            int index = 0;
            for (StructField field : getTableObjectInspector(schema).getAllStructFieldRefs()) {
                if (field.getFieldObjectInspector().getCategory() == ObjectInspector.Category.PRIMITIVE) {
                    PrimitiveObjectInspector inspector = (PrimitiveObjectInspector) field.getFieldObjectInspector();
                    HiveType hiveType = HiveType.getSupportedHiveType(inspector.getPrimitiveCategory());
                    return new HiveColumnHandle(clientId, field.getFieldName(), index, hiveType, index, false);
                }
                index++;
            }
        }
        catch (MetaException | SerDeException | RuntimeException e) {
            throw Throwables.propagate(e);
        }

        throw new IllegalStateException("Table doesn't have any PRIMITIVE columns");
    }

    private static RecordReader<?, ?> createRecordReader(HiveSplit split, Configuration configuration, Path wrappedPath)
    {
        final InputFormat<?, ?> inputFormat = getInputFormat(configuration, split.getSchema(), true);
        final JobConf jobConf = new JobConf(configuration);
        final FileSplit fileSplit = createFileSplit(wrappedPath, split.getStart(), split.getLength());

        try {
            return retry().stopOnIllegalExceptions().run("createRecordReader", new Callable<RecordReader<?, ?>>()
            {
                @Override
                public RecordReader<?, ?> call()
                        throws IOException
                {
                    return inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
                }
            });
        }
        catch (Exception e) {
            throw new RuntimeException(String.format("Error opening Hive split %s (offset=%s, length=%s) using %s: %s",
                    split.getPath(),
                    split.getStart(),
                    split.getLength(),
                    getInputFormatName(split.getSchema()),
                    e.getMessage()),
                    e);
        }
    }

    private static FileSplit createFileSplit(final Path path, long start, long length)
    {
        return new FileSplit(path, start, length, (String[]) null)
        {
            @Override
            public Path getPath()
            {
                // make sure our original path object is returned
                return path;
            }
        };
    }
}
