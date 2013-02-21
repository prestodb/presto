package com.facebook.presto.hive;

import com.facebook.presto.spi.SchemaField;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.ql.io.SymlinkTextInputFormat;
import org.apache.hadoop.hive.serde2.objectinspector.PrimitiveObjectInspector;
import org.apache.hadoop.hive.serde2.objectinspector.primitive.PrimitiveObjectInspectorUtils;
import org.apache.hadoop.mapred.InputFormat;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapred.TextInputFormat;
import org.apache.hadoop.util.ReflectionUtils;

import java.util.Properties;

import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.hive.metastore.api.Constants.FILE_INPUT_FORMAT;

class HiveUtil
{
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

    static SchemaField.Type getSupportedPrimitiveType(PrimitiveObjectInspector.PrimitiveCategory category)
    {
        SchemaField.Type type = getPrimitiveType(category);
        if (type == null) {
            throw new IllegalArgumentException("Hive type not supported: " + category);
        }
        return type;
    }

    static SchemaField.Type getPrimitiveType(PrimitiveObjectInspector.PrimitiveCategory category)
    {
        switch (category) {
            case BYTE:
            case SHORT:
            case INT:
            case LONG:
                return SchemaField.Type.LONG;
            case FLOAT:
            case DOUBLE:
                return SchemaField.Type.DOUBLE;
            case STRING:
                return SchemaField.Type.STRING;
            case BOOLEAN:
                return SchemaField.Type.LONG;
            default:
                return null;
        }
    }

    static SchemaField.Type convertHiveType(String type)
    {
        return getSupportedPrimitiveType(convertNativeHiveType(type));
    }

    static PrimitiveObjectInspector.PrimitiveCategory convertNativeHiveType(String type)
    {
        return PrimitiveObjectInspectorUtils.getTypeEntryFromTypeName(type).primitiveCategory;
    }
}
