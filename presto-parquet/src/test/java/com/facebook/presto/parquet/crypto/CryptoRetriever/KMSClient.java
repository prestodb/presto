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
package com.facebook.presto.parquet.crypto.CryptoRetriever;

import com.facebook.presto.parquet.crypto.DecryptionKeyRetriever;

import java.io.IOException;

public abstract class KMSClient
        implements DecryptionKeyRetriever
{
    /**
     * Generate EKeySet which includes plaintext working key and metadata that includes 1) encrypted working key
     * 2)Master key version 3) IV 4) Master key name
     */
    public abstract EKeySet generateEK(String keyName)
            throws IOException;
    public class EKeySet
    {
        public final byte[] plainTextKey;
        public final byte[] metaData;

        public EKeySet(byte[] plainTextKey, byte[] metaData)
        {
            if (plainTextKey == null || metaData == null) {
                throw new IllegalArgumentException("plainTextKey or metaData cannot be null.");
            }
            this.plainTextKey = plainTextKey;
            this.metaData = metaData;
        }
    }
}
