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
package com.facebook.presto.parquet;

import com.facebook.presto.parquet.cache.MetadataReader;
import com.facebook.presto.parquet.crypto.FileDecryptionProperties;
import com.facebook.presto.parquet.crypto.InternalFileDecryptor;
import com.facebook.presto.parquet.crypto.SampleCryptoPropertiesFactory;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.FileSystem;
import org.apache.hadoop.fs.Path;
import org.apache.parquet.hadoop.metadata.ParquetMetadata;
import org.testng.annotations.Test;

import java.io.IOException;
import java.util.List;

import static org.testng.Assert.assertNotNull;
import static org.testng.Assert.assertTrue;

public class MetadataReaderTest
{
    @Test
    public void testReadFooterEncrypted() throws IOException
    {
        ParquetMetadata parquetMetadata = readFooterWrapper("../src/test/test-data/test-files/encrypted_footer.parquet");
        validate(parquetMetadata);
    }

    @Test
    public void testReadFooterPlainTextWithSignature() throws IOException
    {
        ParquetMetadata parquetMetadata = readFooterWrapper("../src/test/test-data/test-files/plaintext_footer_with_signature.parquet");
        validate(parquetMetadata);
    }

    @Test
    public void testReadFooterPlainText() throws IOException
    {
        ParquetMetadata parquetMetadata = readFooterWrapper("../src/test/test-data/test-files/plaintext_footer.parquet");
        validate(parquetMetadata);
    }

    private ParquetMetadata readFooterWrapper(String file) throws IOException
    {
        Path path = new Path(file);
        Configuration configuration = new Configuration();
        FileSystem fileSystem = path.getFileSystem(configuration);
        fileSystem.setVerifyChecksum(true);
        long fileSize = fileSystem.getFileStatus(path).getLen();
        SampleCryptoPropertiesFactory sampleCryptoPropertiesFactory = new SampleCryptoPropertiesFactory();
        FileDecryptionProperties fileDecryptionProperties = sampleCryptoPropertiesFactory.getFileDecryptionProperties(configuration, path);
        InternalFileDecryptor fileDecryptor = null;
        if (fileDecryptionProperties != null) {
            fileDecryptor = new InternalFileDecryptor(fileDecryptionProperties);
        }

        return MetadataReader.readFooter(fileSystem, path, fileSize, fileDecryptionProperties, fileDecryptor).getParquetMetadata();
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
}
