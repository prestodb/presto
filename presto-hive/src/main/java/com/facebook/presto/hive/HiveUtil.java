package com.facebook.presto.hive;

import com.facebook.presto.spi.ColumnType;
import com.facebook.presto.spi.Partition;
import com.google.common.base.Function;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;
import org.joda.time.format.DateTimeFormat;
import org.joda.time.format.DateTimeFormatter;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.hive.metastore.api.hive_metastoreConstants.FILE_INPUT_FORMAT;

class HiveUtil
{
    public static final DateTimeFormatter HIVE_TIMESTAMP_PARSER = DateTimeFormat.forPattern("yyyy-MM-dd HH:mm:ss.SSSSSSS").withZoneUTC();

    private HiveUtil()
    {
    }

    static InputFormat getInputFormat(Configuration configuration, Properties schema, boolean symlinkTarget)
    {
        String inputFormatName = getInputFormatName(schema);
        try {
            JobConf jobConf = new JobConf(configuration);

            // This code should be equivalent to jobConf.getInputFormat()
            Class<? extends InputFormat> inputFormatClass = jobConf.getClassByName(inputFormatName).asSubclass(InputFormat.class);
            if (inputFormatClass == null) {
                // default file format in Hadoop is TextInputFormat
                inputFormatClass = TextInputFormat.class;
            }
            else if (symlinkTarget && (inputFormatClass == SymlinkTextInputFormat.class)) {
                // symlink targets are always TextInputFormat
                inputFormatClass = TextInputFormat.class;
            }
            return ReflectionUtils.newInstance(inputFormatClass, jobConf);
        }
        catch (Exception e) {
            throw new RuntimeException("Unable to create input format " + inputFormatName, e);
        }
    }

    static String getInputFormatName(Properties schema)
    {
        String name = schema.getProperty(FILE_INPUT_FORMAT);
        checkArgument(name != null, "missing property: %s", FILE_INPUT_FORMAT);
        return name;
    }

    static ColumnType convertHiveType(String type)
    {
        return HiveType.getSupportedHiveType(convertNativeHiveType(type)).getNativeType();
    }

    static PrimitiveObjectInspector.PrimitiveCategory convertNativeHiveType(String type)
    {
        return PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(type).primitiveCategory;
    }

    public static Function<Partition, String> partitionIdGetter()
    {
        return new Function<Partition, String>()
        {
            @Override
            public String apply(Partition input)
            {
                return input.getPartitionId();
            }
        };
    }
}
