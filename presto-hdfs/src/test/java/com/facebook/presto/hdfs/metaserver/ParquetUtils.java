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
//package com.facebook.presto.hdfs.metaserver;
//
//import org.apache.hadoop.conf.Configuration;
//import org.apache.hadoop.fs.Path;
//import org.apache.hadoop.hdfs.DistributedFileSystem;
//import org.apache.parquet.column.ParquetProperties;
//import org.apache.parquet.example.data.simple.SimpleGroupFactory;
//import org.apache.parquet.hadoop.ParquetWriter;
//import org.apache.parquet.hadoop.example.GroupWriteSupport;
//import org.apache.parquet.hadoop.metadata.CompressionCodecName;
//import org.apache.parquet.schema.MessageType;
//import org.apache.parquet.schema.MessageTypeParser;
//import org.testng.annotations.Test;
//
//import java.io.IOException;
//import java.util.Random;
//
///**
// * presto-root
// *
// * @author Jelly
// */
//public class ParquetUtils
//{
//    private Configuration conf = new Configuration();
//    private final String path = "hdfs://127.0.0.1:9000/warehouse/test/student/student03.parquet";
//    private final int blockSize = 256 * 1024 * 1024;
//    private final int pageSize = 6 * 1024;
//    private final int dictionaryPageSize = 512;
//    private final boolean enableDictionary = false;
//    private final boolean validating = false;
//    private final CompressionCodecName compressionCodecName = CompressionCodecName.UNCOMPRESSED;
//    private final ParquetProperties.WriterVersion writerVersion = ParquetProperties.WriterVersion.PARQUET_2_0;
//    private final MessageType schema = MessageTypeParser.parseMessageType("message employee {" +
//            "required binary name; " +
//            "required int32 age; " +
//            "required double salary; " +
//            "required int64 time; " +
//            "required binary comment; " +
//            "}");
//    private GroupWriteSupport groupWriteSupport = new GroupWriteSupport();
//    private SimpleGroupFactory simpleGroupFactory = new SimpleGroupFactory(schema);
//
//    @Test
//    public void write()
//    {
//        conf.set("fs.hdfs.impl", DistributedFileSystem.class.getName());
//        Path file = new Path(path);
//        try {
//            groupWriteSupport.setSchema(schema, conf);
//            ParquetWriter parquetWriter = new ParquetWriter(
//                    file,
//                    groupWriteSupport,
//                    compressionCodecName,
//                    blockSize,
//                    pageSize,
//                    dictionaryPageSize,
//                    enableDictionary,
//                    validating,
//                    writerVersion,
//                    conf
//            );
//            long start = System.currentTimeMillis();
//            for (int i = 0; i < 200000; i++) {
//                parquetWriter.write(
//                        simpleGroupFactory.newGroup()
//                        .append("name", "ron")  //harry, hermione, ron
//                                .append("age", new Random(100L).nextInt(6))
//                        .append("salary", 10.0)
//                        .append("time", System.currentTimeMillis())
//                        .append("comment", "thisis")
//                );
//            }
//            long end = System.currentTimeMillis();
//            System.out.println("Starting: " + start + "\nEnding: " + end);
//            parquetWriter.close();
//        }
//        catch (IOException e) {
//            e.printStackTrace();
//        }
//    }
//
//    @Test
//    public void read()
//    {
//    }
//}
