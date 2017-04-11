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
package com.facebook.presto.hive.benchmark;

import com.facebook.presto.hive.HiveCompressionCodec;
import com.facebook.presto.hive.benchmark.HiveFileFormatBenchmark.CompressionCounter;
import com.facebook.presto.hive.benchmark.HiveFileFormatBenchmark.DataSet;
import org.testng.annotations.Test;

public class TestHiveFileFormatBenchmark
{
    @Test
    public void testSomeFormats()
            throws Exception
    {
        executeBenchmark(DataSet.LINEITEM, HiveCompressionCodec.SNAPPY, FileFormat.PRESTO_RCBINARY);
        executeBenchmark(DataSet.LINEITEM, HiveCompressionCodec.SNAPPY, FileFormat.PRESTO_ORC);
        executeBenchmark(DataSet.LINEITEM, HiveCompressionCodec.SNAPPY, FileFormat.HIVE_RCBINARY);
        executeBenchmark(DataSet.MAP_VARCHAR_DOUBLE, HiveCompressionCodec.SNAPPY, FileFormat.PRESTO_RCBINARY);
        executeBenchmark(DataSet.MAP_VARCHAR_DOUBLE, HiveCompressionCodec.SNAPPY, FileFormat.PRESTO_ORC);
        executeBenchmark(DataSet.MAP_VARCHAR_DOUBLE, HiveCompressionCodec.SNAPPY, FileFormat.HIVE_RCBINARY);
        executeBenchmark(DataSet.LARGE_MAP_VARCHAR_DOUBLE, HiveCompressionCodec.SNAPPY, FileFormat.PRESTO_RCBINARY);
        executeBenchmark(DataSet.LARGE_MAP_VARCHAR_DOUBLE, HiveCompressionCodec.SNAPPY, FileFormat.PRESTO_ORC);
        executeBenchmark(DataSet.LARGE_MAP_VARCHAR_DOUBLE, HiveCompressionCodec.SNAPPY, FileFormat.HIVE_RCBINARY);
    }

    @Test
    public void testAllCompression()
            throws Exception
    {
        for (HiveCompressionCodec codec : HiveCompressionCodec.values()) {
            executeBenchmark(DataSet.LINEITEM, codec, FileFormat.PRESTO_RCBINARY);
        }
    }

    @Test
    public void testAllDataSets()
            throws Exception
    {
        for (DataSet dataSet : DataSet.values()) {
            executeBenchmark(dataSet, HiveCompressionCodec.SNAPPY, FileFormat.PRESTO_RCBINARY);
        }
    }

    private static void executeBenchmark(DataSet dataSet, HiveCompressionCodec codec, FileFormat format)
    {
        HiveFileFormatBenchmark benchmark = new HiveFileFormatBenchmark(dataSet, codec, format);
        try {
            benchmark.setup();
            benchmark.read(new CompressionCounter());
            benchmark.write(new CompressionCounter());
        }
        catch (Exception e) {
            throw new RuntimeException("Failed " + dataSet + " " + codec + " " + format, e);
        }
        finally {
            benchmark.tearDown();
        }
    }
}
