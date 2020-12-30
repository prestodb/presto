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
package com.facebook.presto.hive.util;

import com.facebook.presto.hadoop.FileSystemFactory;
import com.facebook.presto.hive.HiveCompressionCodec;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.mapred.JobConf;
import org.apache.hadoop.mapreduce.lib.output.FileOutputFormat;
import org.apache.orc.OrcConf;
import org.apache.parquet.hadoop.ParquetOutputFormat;

import java.util.Map;

import static com.facebook.hive.orc.OrcConf.ConfVars.HIVE_ORC_COMPRESSION;
import static com.google.common.base.Preconditions.checkArgument;
import static org.apache.hadoop.hive.conf.HiveConf.ConfVars.COMPRESSRESULT;
import static org.apache.hadoop.io.SequenceFile.CompressionType.BLOCK;

public final class ConfigurationUtils
{
    public static final String PAGE_FILE_COMPRESSION = "pagefile.output.compression";
    private static final Configuration INITIAL_CONFIGURATION;

    static {
        Configuration.addDefaultResource("hdfs-default.xml");
        Configuration.addDefaultResource("hdfs-site.xml");

        // must not be transitively reloaded during the future loading of various Hadoop modules
        // all the required default resources must be declared above
        INITIAL_CONFIGURATION = new Configuration(false);
        Configuration defaultConfiguration = new Configuration();
        copy(defaultConfiguration, INITIAL_CONFIGURATION);
    }

    private ConfigurationUtils() {}

    public static Configuration getInitialConfiguration()
    {
        return copy(INITIAL_CONFIGURATION);
    }

    public static Configuration copy(Configuration configuration)
    {
        Configuration copy = new Configuration(false);
        copy(configuration, copy);
        return copy;
    }

    public static void copy(Configuration from, Configuration to)
    {
        for (Map.Entry<String, String> entry : from) {
            to.set(entry.getKey(), entry.getValue());
        }
    }

    public static JobConf toJobConf(Configuration conf)
    {
        if (conf instanceof JobConf) {
            return (JobConf) conf;
        }
        return new JobConf(conf);
    }

    public static JobConf configureCompression(Configuration config, HiveCompressionCodec compression)
    {
        JobConf result;
        // FileSystemFactory is used to hack around the abuse of Configuration as a
        // cache for FileSystem. See FileSystemFactory class for more details.
        //
        // It is caller's responsibility to create a copy if FileSystemFactory is used.
        if (config instanceof FileSystemFactory) {
            checkArgument(config instanceof JobConf, "config is not an instance of JobConf: %s", config.getClass());
            result = (JobConf) config;
        }
        else {
            result = new JobConf(false);
            copy(config, result);
        }
        setCompressionProperties(result, compression);
        return result;
    }

    private static void setCompressionProperties(Configuration config, HiveCompressionCodec compression)
    {
        boolean compressed = compression != HiveCompressionCodec.NONE;
        config.setBoolean(COMPRESSRESULT.varname, compressed);
        config.setBoolean("mapred.output.compress", compressed);
        config.setBoolean(FileOutputFormat.COMPRESS, compressed);
        // For DWRF
        com.facebook.hive.orc.OrcConf.setVar(config, HIVE_ORC_COMPRESSION, compression.getOrcCompressionKind().name());
        // For ORC
        OrcConf.COMPRESS.setString(config, compression.getOrcCompressionKind().name());
        // For RCFile and Text
        if (compression.getCodec().isPresent()) {
            config.set("mapred.output.compression.codec", compression.getCodec().get().getName());
            config.set(FileOutputFormat.COMPRESS_CODEC, compression.getCodec().get().getName());
        }
        else {
            config.unset("mapred.output.compression.codec");
            config.unset(FileOutputFormat.COMPRESS_CODEC);
        }
        // For Parquet
        compression.getParquetCompressionCodec().ifPresent(codec -> config.set(ParquetOutputFormat.COMPRESSION, codec.name()));
        // For SequenceFile
        config.set(FileOutputFormat.COMPRESS_TYPE, BLOCK.toString());
        // For PageFile
        config.set(PAGE_FILE_COMPRESSION, compression.name());
    }
}
