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
package com.facebook.presto.parquet.crypto;

import com.facebook.presto.parquet.ParquetDataSourceId;
import com.facebook.presto.parquet.cache.MetadataReader;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FSDataInputStream;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class ParquetMetaDataUtilsTest
{
    Configuration configuration;

    @BeforeTest
    public void setUp() throws IOException
    {
        configuration = new Configuration();
        configuration.setBoolean("InMemoryKMS", true);
        configuration.set(DecryptionPropertiesFactory.CRYPTO_FACTORY_CLASS_PROPERTY_NAME, SampleCryptoPropertiesFactory.class.getName());
    }

    @Test
    public void testGetParquetMetadataEncrypted() throws IOException
    {
        Path path = new Path("../src/test/test-data/test-files/encrypted_footer.parquet");
        FileSystem fileSystem = path.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path);
        long fileSize = fileSystem.getFileStatus(path).getLen();
        FileDecryptionProperties fileDecryptionProperties = createDecryptionProperties(path, configuration);
        InternalFileDecryptor fileDecryptor = null;
        if (fileDecryptionProperties != null) {
            fileDecryptor = new InternalFileDecryptor(fileDecryptionProperties);
        }
        MetadataReader reader = new MetadataReader();
        ParquetMetadata parquetMetadata = reader.getParquetMetadata(inputStream, new ParquetDataSourceId("ID"), fileSize, false, fileDecryptionProperties, fileDecryptor).getParquetMetadata();
        validate(parquetMetadata);
    }

    @Test
    public void testGetParquetMetadataPlainTextWithSignature() throws IOException
    {
        Path path = new Path("../src/test/test-data/test-files/plaintext_footer_with_signature.parquet");
        FileSystem fileSystem = path.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path);
        long fileSize = fileSystem.getFileStatus(path).getLen();
        FileDecryptionProperties fileDecryptionProperties = createDecryptionProperties(path, configuration);
        InternalFileDecryptor fileDecryptor = null;
        if (fileDecryptionProperties != null) {
            fileDecryptor = new InternalFileDecryptor(fileDecryptionProperties);
        }
        MetadataReader reader = new MetadataReader();
        ParquetMetadata parquetMetadata = reader.getParquetMetadata(inputStream, new ParquetDataSourceId("ID"), fileSize, false, fileDecryptionProperties, fileDecryptor).getParquetMetadata();
        validate(parquetMetadata);
    }

    @Test
    public void testGetParquetMetadataPlainText() throws IOException
    {
        Path path = new Path("../src/test/test-data/test-files/plaintext_footer.parquet");
        FileSystem fileSystem = path.getFileSystem(configuration);
        FSDataInputStream inputStream = fileSystem.open(path);
        long fileSize = fileSystem.getFileStatus(path).getLen();
        FileDecryptionProperties fileDecryptionProperties = createDecryptionProperties(path, configuration);
        InternalFileDecryptor fileDecryptor = null;
        if (fileDecryptionProperties != null) {
            fileDecryptor = new InternalFileDecryptor(fileDecryptionProperties);
        }
        MetadataReader reader = new MetadataReader();
        ParquetMetadata parquetMetadata = reader.getParquetMetadata(inputStream, new ParquetDataSourceId("ID"), fileSize, false, fileDecryptionProperties, fileDecryptor).getParquetMetadata();
        validate(parquetMetadata);
    }

    private void validate(ParquetMetadata parquetMetadata)
    {
        assertNotNull(parquetMetadata);
        assertTrue(parquetMetadata.getBlocks().size() > 0);
        List<String[]> paths = parquetMetadata.getFileMetaData().getSchema().getPaths();
        assertTrue(paths.size() == 2);
        assertTrue(paths.get(0).length == 1 && paths.get(0)[0].equals("price"));
        assertTrue(paths.get(1).length == 1 && paths.get(1)[0].equals("product"));
    }

    private static FileDecryptionProperties createDecryptionProperties(Path file, Configuration hadoopConfig)
    {
        DecryptionPropertiesFactory cryptoFactory = DecryptionPropertiesFactory.loadFactory(hadoopConfig);
        if (null == cryptoFactory) {
            return null;
        }
        return cryptoFactory.getFileDecryptionProperties(hadoopConfig, file);
    }
}
