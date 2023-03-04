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
package com.facebook.presto.parquet.reader;

import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.column.ParquetProperties;
import org.apache.parquet.crypto.FileEncryptionProperties;
import org.apache.parquet.crypto.ParquetCipher;
import org.apache.parquet.example.data.Group;
import org.apache.parquet.example.data.simple.SimpleGroup;
import org.apache.parquet.hadoop.ParquetWriter;
import org.apache.parquet.hadoop.example.ExampleParquetWriter;
import org.apache.parquet.hadoop.example.GroupWriteSupport;
import org.apache.parquet.hadoop.metadata.CompressionCodecName;
import org.apache.parquet.schema.GroupType;
import org.apache.parquet.schema.MessageType;
import org.apache.parquet.schema.PrimitiveType;
import org.apache.parquet.schema.Type;

import java.io.IOException;
import java.nio.file.Files;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Map;
import java.util.concurrent.ThreadLocalRandom;

import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.BINARY;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT32;
import static org.apache.parquet.schema.PrimitiveType.PrimitiveTypeName.INT64;

public class TestFileBuilder
{
    private MessageType schema;
    private Configuration conf;
    private Map<String, String> extraMeta = new HashMap<>();
    private int numRecord = 100000;
    private ParquetProperties.WriterVersion writerVersion = ParquetProperties.WriterVersion.PARQUET_1_0;
    private int pageSize = ParquetProperties.DEFAULT_PAGE_SIZE;
    private String codec = "ZSTD";
    private String[] encryptColumns = {};
    private ParquetCipher cipher = ParquetCipher.AES_GCM_V1;
    private Boolean footerEncryption = false;
    private Boolean dataMaskingTest = false;
    private int rowGroupSize = 8 * 1024 * 1024;
    private boolean dictionaryEnabled;
    private int dictionaryPerValueRepeatCount = 10;
    private int lastUsedValue = ThreadLocalRandom.current().nextInt(10000);

    public TestFileBuilder(Configuration conf, MessageType schema)
    {
        this.conf = conf;
        this.schema = schema;
        conf.set(GroupWriteSupport.PARQUET_EXAMPLE_SCHEMA, schema.toString());
    }

    public TestFileBuilder withNumRecord(int numRecord)
    {
        this.numRecord = numRecord;
        return this;
    }

    public TestFileBuilder withEncrytionAlgorithm(ParquetCipher cipher)
    {
        this.cipher = cipher;
        return this;
    }

    public TestFileBuilder withExtraMeta(Map<String, String> extraMeta)
    {
        this.extraMeta = extraMeta;
        return this;
    }

    public TestFileBuilder withWriterVersion(ParquetProperties.WriterVersion writerVersion)
    {
        this.writerVersion = writerVersion;
        return this;
    }

    public TestFileBuilder withPageSize(int pageSize)
    {
        this.pageSize = pageSize;
        return this;
    }

    public TestFileBuilder withRowGroupSize(int rowGroupSize)
    {
        this.rowGroupSize = rowGroupSize;
        return this;
    }

    public TestFileBuilder withCodec(String codec)
    {
        this.codec = codec;
        return this;
    }

    public TestFileBuilder withEncryptColumns(String[] encryptColumns)
    {
        this.encryptColumns = encryptColumns;
        return this;
    }

    public TestFileBuilder withFooterEncryption()
    {
        this.footerEncryption = true;
        return this;
    }

    public TestFileBuilder withDataMaskingTest()
    {
        this.dataMaskingTest = true;
        return this;
    }

    public TestFileBuilder withDictionaryEnabled()
    {
        this.dictionaryEnabled = true;
        return this;
    }

    public TestFile build()
            throws IOException
    {
        String fileName = createTempFile("test");
        SimpleGroup[] fileContent = createFileContent(schema);
        FileEncryptionProperties encryptionProperties = EncryptDecryptUtil.getFileEncryptionProperties(Arrays.asList(encryptColumns), cipher, footerEncryption, dataMaskingTest);
        ExampleParquetWriter.Builder builder = ExampleParquetWriter.builder(new Path(fileName))
                .withConf(conf)
                .withWriterVersion(writerVersion)
                .withExtraMetaData(extraMeta)
                .withValidation(true)
                .withPageSize(pageSize)
                .withRowGroupSize(rowGroupSize)
                .withEncryption(encryptionProperties)
                .withDictionaryEncoding(dictionaryEnabled)
                .withCompressionCodec(CompressionCodecName.valueOf(codec));
        try (ParquetWriter writer = builder.build()) {
            for (int i = 0; i < fileContent.length; i++) {
                writer.write(fileContent[i]);
            }
        }
        return new TestFile(fileName, fileContent);
    }

    private SimpleGroup[] createFileContent(MessageType schema)
    {
        SimpleGroup[] simpleGroups = new SimpleGroup[numRecord];
        for (int i = 0; i < simpleGroups.length; i++) {
            SimpleGroup g = new SimpleGroup(schema);
            for (Type type : schema.getFields()) {
                addValueToSimpleGroup(g, type);
            }
            simpleGroups[i] = g;
        }
        return simpleGroups;
    }

    private void addValueToSimpleGroup(Group g, Type type)
    {
        if (type.isPrimitive()) {
            PrimitiveType primitiveType = (PrimitiveType) type;
            if (primitiveType.getPrimitiveTypeName().equals(INT32)) {
                g.add(type.getName(), getInt());
            }
            else if (primitiveType.getPrimitiveTypeName().equals(INT64)) {
                g.add(type.getName(), getLong());
            }
            else if (primitiveType.getPrimitiveTypeName().equals(BINARY)) {
                g.add(type.getName(), getString());
            }
            // Only support 3 types now, more can be added later
        }
        else {
            GroupType groupType = (GroupType) type;
            Group parentGroup = g.addGroup(groupType.getName());
            for (Type field : groupType.getFields()) {
                addValueToSimpleGroup(parentGroup, field);
            }
        }
    }

    private int getInt()
    {
        if (dictionaryEnabled && dictionaryPerValueRepeatCount > 0) {
            dictionaryPerValueRepeatCount--;
        }
        else {
            dictionaryPerValueRepeatCount = 10;
            lastUsedValue = ThreadLocalRandom.current().nextInt(10000);
        }
        return lastUsedValue;
    }

    private static long getLong()
    {
        return ThreadLocalRandom.current().nextLong(100000);
    }

    private static String getString()
    {
        char[] chars = {'a', 'b', 'c', 'd', 'e', 'f', 'g', 'x', 'z', 'y'};
        StringBuilder sb = new StringBuilder();
        for (int i = 0; i < 100; i++) {
            sb.append(chars[ThreadLocalRandom.current().nextInt(10)]);
        }
        return sb.toString();
    }

    public static String createTempFile(String prefix)
    {
        try {
            return Files.createTempDirectory(prefix).toAbsolutePath().toString() + "/test.parquet";
        }
        catch (IOException e) {
            throw new AssertionError("Unable to create temporary file", e);
        }
    }
}
