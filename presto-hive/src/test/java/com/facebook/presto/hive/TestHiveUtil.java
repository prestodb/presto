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

import com.facebook.presto.hive.authentication.NoHdfsAuthentication;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.StorageFormat;
import com.facebook.presto.hive.metastore.file.FileHiveMetastore;
import com.facebook.presto.hive.util.HudiRealtimeSplitConverter;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;
import com.uber.hoodie.hadoop.realtime.HoodieRealtimeInputFormat;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.hive.metastore.Warehouse;
import org.apache.hadoop.hive.metastore.api.MetaException;
import org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe;
import org.apache.hadoop.hive.serde2.thrift.ThriftDeserializer;
import org.apache.hadoop.hive.serde2.thrift.test.IntString;
import org.apache.hudi.hadoop.HoodieParquetInputFormat;
import org.apache.hudi.hadoop.realtime.HoodieRealtimeFileSplit;
import org.apache.thrift.protocol.TBinaryProtocol;
import org.joda.time.DateTime;
import org.joda.time.DateTimeZone;
import org.joda.time.format.DateTimeFormat;
import org.testng.annotations.Test;

import java.io.File;
import java.util.AbstractList;
import java.util.ArrayList;
import java.util.Collections;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Properties;

import static com.facebook.airlift.testing.Assertions.assertInstanceOf;
import static com.facebook.presto.hive.HiveUtil.getDeserializer;
import static com.facebook.presto.hive.HiveUtil.parseHiveTimestamp;
import static com.facebook.presto.hive.HiveUtil.shouldUseRecordReaderFromInputFormat;
import static com.facebook.presto.hive.metastore.MetastoreUtil.toPartitionValues;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_CLASS;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_FORMAT;
import static org.apache.hadoop.hive.serde.serdeConstants.SERIALIZATION_LIB;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestHiveUtil
{
    public static FileHiveMetastore createTestingFileHiveMetastore(File catalogDirectory)
    {
        HiveClientConfig hiveClientConfig = new HiveClientConfig();
        MetastoreClientConfig metastoreClientConfig = new MetastoreClientConfig();
        HdfsConfiguration hdfsConfiguration = new HiveHdfsConfiguration(new HdfsConfigurationInitializer(hiveClientConfig, metastoreClientConfig), ImmutableSet.of());
        HdfsEnvironment hdfsEnvironment = new HdfsEnvironment(hdfsConfiguration, metastoreClientConfig, new NoHdfsAuthentication());
        return new FileHiveMetastore(hdfsEnvironment, catalogDirectory.toURI().toString(), "test");
    }

    @Test
    public void testParseHiveTimestamp()
    {
        DateTime time = new DateTime(2011, 5, 6, 7, 8, 9, 123, nonDefaultTimeZone());
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss"), unixTime(time, 0));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.S"), unixTime(time, 1));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSS"), unixTime(time, 3));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSSSSSS"), unixTime(time, 6));
        assertEquals(parse(time, "yyyy-MM-dd HH:mm:ss.SSSSSSSSS"), unixTime(time, 7));
    }

    @Test
    public void testGetThriftDeserializer()
    {
        Properties schema = new Properties();
        schema.setProperty(SERIALIZATION_LIB, ThriftDeserializer.class.getName());
        schema.setProperty(SERIALIZATION_CLASS, IntString.class.getName());
        schema.setProperty(SERIALIZATION_FORMAT, TBinaryProtocol.class.getName());

        assertInstanceOf(getDeserializer(new Configuration(false), schema), ThriftDeserializer.class);
    }

    @Test
    public void testToPartitionValues()
            throws MetaException
    {
        assertToPartitionValues("ds=2015-12-30/event_type=QueryCompletion");
        assertToPartitionValues("ds=2015-12-30");
        assertToPartitionValues("a=1/b=2/c=3");
        assertToPartitionValues("a=1");
        assertToPartitionValues("pk=!@%23$%25%5E&%2A()%2F%3D");
        assertToPartitionValues("pk=__HIVE_DEFAULT_PARTITION__");
    }

    @Test
    public void testShouldUseRecordReaderFromInputFormat()
    {
        StorageFormat hudiStorageFormat = StorageFormat.create(ParquetHiveSerDe.class.getName(),
                HoodieParquetInputFormat.class.getName(), "");
        assertFalse(shouldUseRecordReaderFromInputFormat(new Configuration(),
                new Storage(hudiStorageFormat, "test", Optional.empty(), true, ImmutableMap.of(),
                        ImmutableMap.of()), Collections.emptyMap()));

        StorageFormat hudiRealtimeStorageFormat = StorageFormat.create(ParquetHiveSerDe.class.getName(),
                HoodieRealtimeInputFormat.class.getName(), "");
        Map<String, String> customSplitInfo = ImmutableMap.of(
                HiveUtil.CUSTOM_FILE_SPLIT_CLASS_KEY, HoodieRealtimeFileSplit.class.getName(),
                HudiRealtimeSplitConverter.HUDI_BASEPATH_KEY, "/test/file.parquet",
                HudiRealtimeSplitConverter.HUDI_DELTA_FILEPATHS_KEY, "/test/.file_100.log",
                HudiRealtimeSplitConverter.HUDI_MAX_COMMIT_TIME_KEY, "100");
        assertTrue(shouldUseRecordReaderFromInputFormat(new Configuration(),
                new Storage(hudiRealtimeStorageFormat, "test", Optional.empty(), true, ImmutableMap.of(),
                        ImmutableMap.of()), customSplitInfo));
    }

    private static void assertToPartitionValues(String partitionName)
            throws MetaException
    {
        List<String> actual = toPartitionValues(partitionName);
        AbstractList<String> expected = new ArrayList<>();
        for (String s : actual) {
            expected.add(null);
        }
        Warehouse.makeValsFromName(partitionName, expected);
        assertEquals(actual, expected);
    }

    private static long parse(DateTime time, String pattern)
    {
        return parseHiveTimestamp(DateTimeFormat.forPattern(pattern).print(time), nonDefaultTimeZone());
    }

    private static long unixTime(DateTime time, int factionalDigits)
    {
        int factor = (int) Math.pow(10, Math.max(0, 3 - factionalDigits));
        return (time.getMillis() / factor) * factor;
    }

    static DateTimeZone nonDefaultTimeZone()
    {
        String defaultId = DateTimeZone.getDefault().getID();
        for (String id : DateTimeZone.getAvailableIDs()) {
            if (!id.equals(defaultId)) {
                DateTimeZone zone = DateTimeZone.forID(id);
                if (zone.getStandardOffset(0) != 0) {
                    return zone;
                }
            }
        }
        throw new IllegalStateException("no non-default timezone");
    }
}
