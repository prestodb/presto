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

package com.facebook.presto.hudi;

import com.facebook.presto.common.type.TypeManager;
import com.facebook.presto.hive.HdfsContext;
import com.facebook.presto.hive.HdfsEnvironment;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.RecordCursor;
import com.google.common.base.Joiner;
import com.google.common.base.Splitter;
import io.airlift.compress.lzo.LzoCodec;
import io.airlift.compress.lzo.LzopCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.io.Writable;
import org.apache.hadoop.mapred.FileSplit;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.RecordReader;
import org.apache.hadoop.mapred.Reporter;
import org.apache.hadoop.util.ReflectionUtils;
import org.apache.hudi.common.model.HoodieLogFile;
import org.apache.hudi.common.util.Option;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;

import java.io.IOException;
import java.time.ZoneId;
import java.util.List;
import java.util.Properties;
import java.util.function.Function;

import static com.facebook.presto.hive.HudiRecordCursors.createRecordCursor;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_CANNOT_OPEN_SPLIT;
import static com.facebook.presto.hudi.HudiErrorCode.HUDI_FILESYSTEM_ERROR;
import static com.google.common.base.MoreObjects.firstNonNull;
import static com.google.common.base.Preconditions.checkArgument;
import static com.google.common.collect.Lists.newArrayList;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;
import static java.util.stream.Collectors.toList;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_ALL_COLUMNS;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_IDS_CONF_STR;
import static org.apache.hadoop.hive.serde2.ColumnProjectionUtils.READ_COLUMN_NAMES_CONF_STR;
import static org.apache.hudi.common.config.HoodieReaderConfig.FILE_GROUP_READER_ENABLED;

class HudiRecordCursors
{
    private HudiRecordCursors() {}

    public static RecordCursor createRealtimeRecordCursor(
            HdfsEnvironment hdfsEnvironment,
            ConnectorSession session,
            Properties schema,
            HudiSplit split,
            List<HudiColumnHandle> dataColumns,
            ZoneId hiveStorageTimeZone,
            TypeManager typeManager)
    {
        requireNonNull(session, "session is null");
        checkArgument(dataColumns.stream().allMatch(HudiRecordCursors::isRegularColumn), "dataColumns contains non regular column");
        HudiFile baseFile = getHudiBaseFile(split);
        Path path = new Path(baseFile.getPath());

        HdfsContext context = new HdfsContext(session,
                split.getTable().getSchemaName(),
                split.getTable().getTableName(),
                baseFile.getPath(),
                false);
        Configuration conf = null;
        try {
            conf = hdfsEnvironment.getFileSystem(context, path).getConf();
        }
        catch (IOException e) {
            throw new PrestoException(HUDI_FILESYSTEM_ERROR, "Could not open file system for " + split.getTable(), e);
        }
        final Configuration configuration = conf;
        return hdfsEnvironment.doAs(session.getUser(), () -> {
            RecordReader<?, ?> recordReader = createRecordReader(configuration, schema, split, dataColumns);
            @SuppressWarnings("unchecked") RecordReader<?, ? extends Writable> reader = (RecordReader<?, ? extends Writable>) recordReader;
            return createRecordCursor(configuration, path, reader, baseFile.getLength(), schema, dataColumns, hiveStorageTimeZone, typeManager);
        });
    }

    private static RecordReader<?, ?> createRecordReader(
            Configuration configuration,
            Properties schema,
            HudiSplit split,
            List<HudiColumnHandle> dataColumns)
    {
        // update configuration
        JobConf jobConf = new JobConf(configuration);
        jobConf.setBoolean(READ_ALL_COLUMNS, false);
        jobConf.set(READ_COLUMN_IDS_CONF_STR, join(dataColumns, HudiColumnHandle::getId));
        jobConf.set(READ_COLUMN_NAMES_CONF_STR, join(dataColumns, HudiColumnHandle::getName));
        jobConf.setBoolean(FILE_GROUP_READER_ENABLED.key(), false);
        schema.stringPropertyNames()
                .forEach(name -> jobConf.set(name, schema.getProperty(name)));
        refineCompressionCodecs(jobConf);

        // create input format
        String inputFormatName = split.getPartition().getStorage().getStorageFormat().getInputFormat();
        InputFormat<?, ?> inputFormat = createInputFormat(jobConf, inputFormatName);

        // create record reader for split
        try {
            HudiFile baseFile = getHudiBaseFile(split);
            Path path = new Path(baseFile.getPath());
            FileSplit fileSplit = new FileSplit(path, baseFile.getStart(), baseFile.getLength(), (String[]) null);
            List<HoodieLogFile> logFiles = split.getLogFiles().stream().map(file -> new HoodieLogFile(file.getPath())).collect(toList());
            String tablePath = split.getTable().getPath();
            FileSplit hudiSplit = new HoodieRealtimeFileSplit(fileSplit, tablePath, logFiles, split.getInstantTime(), false, Option.empty());
            return inputFormat.getRecordReader(hudiSplit, jobConf, Reporter.NULL);
        }
        catch (IOException e) {
            String msg = format("Error opening Hive split %s using %s: %s",
                    split,
                    inputFormatName,
                    firstNonNull(e.getMessage(), e.getClass().getName()));
            throw new PrestoException(HUDI_CANNOT_OPEN_SPLIT, msg, e);
        }
    }

    private static InputFormat<?, ?> createInputFormat(Configuration conf, String inputFormat)
    {
        try {
            Class<?> clazz = conf.getClassByName(inputFormat);
            @SuppressWarnings("unchecked") Class<? extends InputFormat<?, ?>> cls =
                    (Class<? extends InputFormat<?, ?>>) clazz.asSubclass(InputFormat.class);
            return ReflectionUtils.newInstance(cls, conf);
        }
        catch (ClassNotFoundException | RuntimeException e) {
            throw new PrestoException(HUDI_CANNOT_OPEN_SPLIT, "Unable to create input format " + inputFormat, e);
        }
    }

    private static void refineCompressionCodecs(Configuration conf)
    {
        List<String> codecs = newArrayList(Splitter.on(",").trimResults().omitEmptyStrings()
                .split(conf.get("io.compression.codecs", "")));
        if (!codecs.contains(LzoCodec.class.getName())) {
            codecs.add(0, LzoCodec.class.getName());
        }
        if (!codecs.contains(LzopCodec.class.getName())) {
            codecs.add(0, LzopCodec.class.getName());
        }
        conf.set("io.compression.codecs", String.join(",", codecs));
    }

    private static <T, V> String join(List<T> list, Function<T, V> extractor)
    {
        return Joiner.on(',').join(list.stream().map(extractor).iterator());
    }

    private static boolean isRegularColumn(HudiColumnHandle column)
    {
        return column.getColumnType() == HudiColumnHandle.ColumnType.REGULAR;
    }
    private static HudiFile getHudiBaseFile(HudiSplit hudiSplit)
    {
        // use first log file as base file for MOR table if it hasn't base file
        if (hudiSplit.getBaseFile().isPresent()) {
            return hudiSplit.getBaseFile().get();
        }
        else {
            return hudiSplit.getLogFiles().get(0);
        }
    }
}
