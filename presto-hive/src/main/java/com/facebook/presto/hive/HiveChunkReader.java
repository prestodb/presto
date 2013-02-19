package com.facebook.presto.hive;

import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.SchemaField;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.hive.metastore.MetaStoreUtils;
import org.apache.hadoop.hive.serde.Constants;
import org.apache.hadoop.hive.serde2.ColumnProjectionUtils;
import org.apache.hadoop.hive.serde2.Deserializer;
import org.apache.hadoop.hive.serde2.columnar.BytesRefArrayWritable;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.StructField;
import org.apache.hadoop.hive.serde2.objectinspector.StructObjectInspector;
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

import static com.facebook.presto.hive.HadoopConfiguration.HADOOP_CONFIGURATION;
import static com.facebook.presto.hive.HiveColumn.indexGetter;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.facebook.presto.hive.HiveUtil.getInputFormatName;
import static com.facebook.presto.hive.HiveUtil.getSupportedPrimitiveType;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Lists.transform;

class HiveChunkReader
{
    private HiveChunkReader()
    {
    }

    static RecordCursor getRecords(HivePartitionChunk chunk)
    {
        HadoopNative.requireHadoopNative();

        try {
            // Clone schema since we modify it below
            Properties schema = (Properties) chunk.getSchema().clone();

            // We are handling parsing directly since the hive code is slow
            // In order to do this, remove column types entry so that hive treats all columns as type "string"
            String typeSpecification = (String) schema.remove(Constants.LIST_COLUMN_TYPES);
            Preconditions.checkNotNull(typeSpecification, "Partition column type specification is null");

            String nullSequence = (String) schema.get(Constants.SERIALIZATION_NULL_FORMAT);
            checkState(nullSequence == null || nullSequence.equals("\\N"), "Only '\\N' supported as null specifier, was '%s'", nullSequence);

            // Tell hive the columns we would like to read, this lets hive optimize reading column oriented files
            List<HiveColumn> columns = chunk.getColumns();
            if (columns.isEmpty()) {
                // for count(*) queries we will have "no" columns we want to read, but since hive doesn't
                // support no columns (it will read all columns instead), we must choose a single column
                columns = ImmutableList.of(getFirstPrimitiveColumn(schema));
            }
            ColumnProjectionUtils.setReadColumnIDs(HADOOP_CONFIGURATION.get(), new ArrayList<>(transform(columns, indexGetter())));

            RecordReader<?, ?> recordReader = createRecordReader(chunk);
            if (recordReader.createValue() instanceof BytesRefArrayWritable) {
                return new BytesHiveRecordCursor<>((RecordReader<?, BytesRefArrayWritable>) recordReader, chunk.getLength(), chunk.getPartitionKeys(), columns);
            }
            else {
                return new GenericHiveRecordCursor<>((RecordReader<?, ? extends Writable>) recordReader, chunk.getLength(), chunk.getSchema(), chunk.getPartitionKeys(), columns);
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static HiveColumn getFirstPrimitiveColumn(Properties schema)
    {
        try {
            Deserializer deserializer = MetaStoreUtils.getDeserializer(null, schema);
            StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

            int index = 0;
            for (StructField field : rowInspector.getAllStructFieldRefs()) {
                if (field.getFieldObjectInspector().getCategory() == ObjectInspector.Category.PRIMITIVE) {
                    PrimitiveObjectInspector inspector = (PrimitiveObjectInspector) field.getFieldObjectInspector();
                    SchemaField.Type type = getSupportedPrimitiveType(inspector.getPrimitiveCategory());
                    return new HiveColumn(field.getFieldName(), index, type, inspector.getPrimitiveCategory());
                }
                index++;
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        throw new IllegalStateException("Table doesn't have any PRIMITIVE columns");
    }

    private static RecordReader<?, ?> createRecordReader(HivePartitionChunk chunk)
    {
        InputFormat inputFormat = getInputFormat(chunk.getSchema(), true);
        FileSplit split = new FileSplit(chunk.getPath(), chunk.getStart(), chunk.getLength(), (String[]) null);
        JobConf jobConf = new JobConf(HADOOP_CONFIGURATION.get());

        try {
            return inputFormat.getRecordReader(split, jobConf, Reporter.NULL);
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to create record reader for input format " + getInputFormatName(chunk.getSchema()), e);
        }
    }
}
