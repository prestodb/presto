package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.RecordCursor;
import com.facebook.presto.spi.RecordSet;
import com.google.common.base.Preconditions;
import com.google.common.base.Throwables;
import com.google.common.collect.ImmutableList;
import org.apache.hadoop.fs.Path;
import com.google.common.collect.Iterables;
import org.apache.hadoop.conf.Configuration;
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

import static com.facebook.presto.hive.HiveColumnHandle.hiveColumnIndexGetter;
import static com.facebook.presto.hive.HiveColumnHandle.nativeTypeGetter;
import static com.facebook.presto.hive.HiveColumnHandle.partitionColumnPredicate;
import static com.facebook.presto.hive.HiveUtil.getInputFormat;
import static com.facebook.presto.hive.HiveUtil.getInputFormatName;
import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.Iterables.filter;
import static com.google.common.collect.Lists.transform;

public class HiveRecordSet
        implements RecordSet
{
    static{
        HadoopNative.requireHadoopNative();
    }

    private final HdfsEnvironment hdfsEnvironment;
    private final HiveSplit split;
    private final List<HiveColumnHandle> columns;
    private final List<ColumnType> columnTypes;
    private final ArrayList<Integer> readHiveColumnIndexes;

    public HiveRecordSet(HdfsEnvironment hdfsEnvironment, HiveSplit split, List<HiveColumnHandle> columns)
    {
        this.hdfsEnvironment = Preconditions.checkNotNull(hdfsEnvironment, "hdfsEnvironment is null");
        this.split = split;
        this.columns = columns;
        this.columnTypes = ImmutableList.copyOf(Iterables.transform(columns, nativeTypeGetter()));

        // determine which hive columns we will read
        List<HiveColumnHandle> readColumns = ImmutableList.copyOf(filter(columns, partitionColumnPredicate()));
        if (readColumns.isEmpty()) {
            // for count(*) queries we will have "no" columns we want to read, but since hive doesn't
            // support no columns (it will read all columns instead), we must choose a single column
            HiveColumnHandle primitiveColumn = getFirstPrimitiveColumn(split.getClientId(), split.getSchema());
            readColumns = ImmutableList.of(primitiveColumn);
        }
        readHiveColumnIndexes = new ArrayList<>(transform(readColumns, hiveColumnIndexGetter()));
    }

    @Override
    public List<ColumnType> getColumnTypes()
    {
        return columnTypes;
    }

    @Override
    public RecordCursor cursor()
    {
        try {
            // Clone schema since we modify it below
            Properties schema = (Properties) split.getSchema().clone();

            // We are handling parsing directly since the hive code is slow
            // In order to do this, remove column types entry so that hive treats all columns as type "string"
            String typeSpecification = (String) schema.remove(Constants.LIST_COLUMN_TYPES);
            Preconditions.checkNotNull(typeSpecification, "Partition column type specification is null");

            String nullSequence = (String) schema.get(Constants.SERIALIZATION_NULL_FORMAT);
            checkState(nullSequence == null || nullSequence.equals("\\N"), "Only '\\N' supported as null specifier, was '%s'", nullSequence);

            Configuration configuration = hdfsEnvironment.getConfiguration();

            // Tell hive the columns we would like to read, this lets hive optimize reading column oriented files
            ColumnProjectionUtils.setReadColumnIDs(configuration, readHiveColumnIndexes);

            RecordReader<?, ?> recordReader = createRecordReader(configuration, split);
            if (recordReader.createValue() instanceof BytesRefArrayWritable) {
                return new BytesHiveRecordCursor<>((RecordReader<?, BytesRefArrayWritable>) recordReader,
                        split.getLength(),
                        split.getSchema(),
                        split.getPartitionKeys(),
                        columns);
            }
            else {
                return new GenericHiveRecordCursor<>((RecordReader<?, ? extends Writable>) recordReader,
                        split.getLength(),
                        split.getSchema(),
                        split.getPartitionKeys(),
                        columns);
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }
    }

    private static HiveColumnHandle getFirstPrimitiveColumn(String clientId, Properties schema)
    {
        try {
            Deserializer deserializer = MetaStoreUtils.getDeserializer(null, schema);
            StructObjectInspector rowInspector = (StructObjectInspector) deserializer.getObjectInspector();

            int index = 0;
            for (StructField field : rowInspector.getAllStructFieldRefs()) {
                if (field.getFieldObjectInspector().getCategory() == ObjectInspector.Category.PRIMITIVE) {
                    PrimitiveObjectInspector inspector = (PrimitiveObjectInspector) field.getFieldObjectInspector();
                    HiveType hiveType = HiveType.getSupportedHiveType(inspector.getPrimitiveCategory());
                    return new HiveColumnHandle(clientId, field.getFieldName(), index, hiveType, index, false);
                }
                index++;
            }
        }
        catch (Exception e) {
            throw Throwables.propagate(e);
        }

        throw new IllegalStateException("Table doesn't have any PRIMITIVE columns");
    }

    private static RecordReader<?, ?> createRecordReader(Configuration configuration, HiveSplit split)
    {
        InputFormat inputFormat = getInputFormat(configuration, split.getSchema(), true);
        FileSplit fileSplit = new FileSplit(new Path(split.getPath()), split.getStart(), split.getLength(), (String[]) null);
        JobConf jobConf = new JobConf(configuration);

        try {
            return inputFormat.getRecordReader(fileSplit, jobConf, Reporter.NULL);
        }
        catch (IOException e) {
            throw new RuntimeException("Unable to create record reader for input format " + getInputFormatName(split.getSchema()), e);
        }
    }
}
