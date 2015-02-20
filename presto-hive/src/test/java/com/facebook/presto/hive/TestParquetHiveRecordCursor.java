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

import com.facebook.presto.spi.type.StandardTypes;
import com.facebook.presto.spi.type.Type;
import com.facebook.presto.spi.type.TypeManager;
import com.facebook.presto.spi.type.TypeSignature;
import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.hadoop.mapred.JobConf;
import org.testng.annotations.Test;
import parquet.hadoop.ParquetRecordReader;
import parquet.schema.GroupType;
import parquet.schema.PrimitiveType;

import java.util.ArrayList;
import java.util.List;
import java.util.Properties;

import static com.facebook.presto.hive.HiveType.HIVE_STRING;
import static com.facebook.presto.hive.ParquetHiveRecordCursor.ParquetStructJsonConverter;
import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;
import static parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static parquet.schema.Type.Repetition.OPTIONAL;

public class TestParquetHiveRecordCursor
{
    class MockParquetHiveRecordCursor extends ParquetHiveRecordCursor
    {
        public MockParquetHiveRecordCursor(Configuration configuration, Path path, long start, long length, Properties splitSchema, List<HivePartitionKey> partitionKeys, List<HiveColumnHandle> columns, TypeManager typeManager)
        {
            super(configuration, path, start, length, splitSchema, partitionKeys, columns, typeManager);
        }

        @Override
        public ParquetRecordReader<Void> createParquetRecordReader(Configuration configuration, Path path, long start, long length, List<HiveColumnHandle> columns)
        {
            //avoid reading from the file, etc.
            return null;
        }
    }

    @Test
    public void testParquetStructRead()
    {
        HiveColumnHandle testColumn = new HiveColumnHandle("client", "name", 11, HIVE_STRING, parseTypeSignature(StandardTypes.VARCHAR), 11, false);

        ParquetHiveRecordCursor c = new MockParquetHiveRecordCursor(new JobConf(),
                new Path("/tmp/test"),
                0,
                0,
                new Properties(),
                new ArrayList<>(),
                ImmutableList.of(testColumn),
                new TypeManager() {
                    @Override
                    public Type getType(TypeSignature signature)
                    {
                        return VARCHAR;
                    }

                    @Override
                    public Type getParameterizedType(String baseTypeName, List<TypeSignature> typeParameters, List<Object> literalParameters)
                    {
                        return null;
                    }

                    @Override
                    public List<Type> getTypes()
                    {
                        return ImmutableList.of();
                    }
        });

        ParquetHiveRecordCursor.ParquetJsonColumnConverter converter;
        PrimitiveType binaryColumn = new PrimitiveType(OPTIONAL, BINARY, null);
        final GroupType groupType = new GroupType(OPTIONAL, "foo", binaryColumn);
        ParquetStructJsonConverter parquetStructJsonConverter = new ParquetStructJsonConverter("test_col", null, groupType);
        converter = c.new ParquetJsonColumnConverter(parquetStructJsonConverter, 0);
        converter.start();
        converter.end();
        Slice s = c.getSlice(0);
        assertEquals(s.toStringUtf8(), "[null]");
    }
}
