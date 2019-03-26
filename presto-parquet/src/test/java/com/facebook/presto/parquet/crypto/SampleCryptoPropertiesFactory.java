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

import com.facebook.presto.parquet.crypto.CryptoRetriever.InMemoryKMSClient;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.fs.Path;

import java.io.IOException;

public class SampleCryptoPropertiesFactory
        implements DecryptionPropertiesFactory
{
    private static final byte[] FOOTER_KEY = {0x01, 0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a,
            0x0b, 0x0c, 0x0d, 0x0e, 0x0f, 0x10};
    private static final byte[] COL_KEY = {0x02, 0x03, 0x4, 0x05, 0x06, 0x07, 0x08, 0x09, 0x0a, 0x0b,
            0x0c, 0x0d, 0x0e, 0x0f, 0x10, 0x11};

    @Override
    public FileDecryptionProperties getFileDecryptionProperties(Configuration hadoopConfig, Path filePath)
            throws ParquetCryptoRuntimeException
    {
        InMemoryKMSClient keyRetriever = null;
        try {
            keyRetriever = new InMemoryKMSClient(new Configuration());
            keyRetriever.putKey("footkey".getBytes(), FOOTER_KEY);
            keyRetriever.putKey("col".getBytes(), COL_KEY);
        }
        catch (IOException e) {
            throw new ParquetCryptoRuntimeException(e);
        }

        return FileDecryptionProperties.builder().withPlaintextFilesAllowed().withoutFooterSignatureVerification().withKeyRetriever(keyRetriever).build();
    }
}
