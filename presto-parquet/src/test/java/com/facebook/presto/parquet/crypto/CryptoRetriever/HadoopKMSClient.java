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

import com.facebook.presto.parquet.crypto.CryptoRetriever.KeyMetadataAssembler.MetadataType;
import com.facebook.presto.parquet.crypto.KeyAccessDeniedException;
import com.facebook.presto.parquet.crypto.ParquetCryptoRuntimeException;
import org.apache.hadoop.conf.Configuration;
import org.apache.hadoop.crypto.key.KeyProvider.KeyVersion;
import org.apache.hadoop.crypto.key.KeyProviderCryptoExtension.EncryptedKeyVersion;
import org.apache.hadoop.crypto.key.kms.KMSClientProvider;
import org.apache.hadoop.crypto.key.kms.LoadBalancingKMSClientProvider;
import org.apache.hadoop.security.AccessControlException;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import java.io.IOException;
import java.io.InvalidObjectException;
import java.net.URI;
import java.net.URISyntaxException;
import java.security.GeneralSecurityException;
import java.security.InvalidParameterException;

/**
 * This class can be used when the KMS client is Hadoop compatible one. e.g. Ranger KMS.
 */
public class HadoopKMSClient
        extends KMSClient
{
    private static Logger log = LoggerFactory.getLogger(HadoopKMSClient.class);

    private LoadBalancingKMSClientProvider keyProvider;

    public HadoopKMSClient(String url, Configuration conf)
            throws IOException, URISyntaxException
    {
        KMSClientProvider.Factory factory = new KMSClientProvider.Factory();
        this.keyProvider = (LoadBalancingKMSClientProvider) factory.createProvider(new URI(url), conf);
    }

    /**
     * Key length must be either 16, 24 or 32 bytes. For reading path.
     * Given parquet footer metadata, return the column key to read with.
     * @param keyMetadata byte array holding key metadata.
     * @return Column key to decrypt parquet file.
     * @throws IOException
     */
    public byte[] getKey(byte[] keyMetadata)
            throws KeyAccessDeniedException, ParquetCryptoRuntimeException
    {
        try {
            return getKeyInternal(keyMetadata);
        }
        catch (IOException e) {
            throw new ParquetCryptoRuntimeException(e);
        }
    }

    public byte[] getKeyInternal(byte[] keyMetadata)
            throws KeyAccessDeniedException, IOException
    {
        int offset = 0;
        if (keyMetadata[offset] != KeyMetadataAssembler.ASSEMBLER_VERSION) {
            throw new IllegalArgumentException("Illegal keyMetadata assembler version");
        }
        // Loop through metadata, for each type call assembler with offset.
        offset = 1; // Start from 1 because offset 0 is the assembler version.
        byte[] iv = null;
        int version = 0;
        byte[] eek = null;
        String mkName = null;
        while (offset < keyMetadata.length) {
            if (keyMetadata[offset] == MetadataType.IV.getVal()) {
                iv = KeyMetadataAssembler.getIv(keyMetadata, offset + 1);
            }
            else if (keyMetadata[offset] == MetadataType.KEYVERSION.getVal()) {
                version = KeyMetadataAssembler.getVersion(keyMetadata, offset + 1);
            }
            else if (keyMetadata[offset] == MetadataType.EEK.getVal()) {
                eek = KeyMetadataAssembler.getEEK(keyMetadata, offset + 1);
            }
            else if (keyMetadata[offset] == MetadataType.MKNAME.getVal()) {
                mkName = KeyMetadataAssembler.getName(keyMetadata, offset + 1);
            }
            else {
                throw new InvalidParameterException("MetadataType does not exist");
            }
            int dataLength = KeyMetadataAssembler.getDataLength(keyMetadata, offset + 1);
            offset += (KeyMetadataAssembler.LENGTH_BYTES + dataLength + 1); // Add 1 byte for type byte.
        }

        String versionName = mkName + "@" + version;
        log.info("Preparing to decrypt EEK using master key name = " + versionName);

        EncryptedKeyVersion encryptedKeyVersion = EncryptedKeyVersion.createForDecryption(mkName, versionName, iv, eek);

        try {
            KeyVersion keyVersion = this.keyProvider.decryptEncryptedKey(encryptedKeyVersion);
            return keyVersion.getMaterial();
        }
        catch (AccessControlException ace) {
            throw new KeyAccessDeniedException(mkName);
        }
        catch (GeneralSecurityException se) {
            throw new IOException(se);
        }
    }

    /**
     * Generate EKeySet which includes
     * 0) Plaintext working key
     * 1) encrypted working key
     * 2) Master key version
     * 3) IV
     * 4) Master key name
     */
    public EKeySet generateEK(String keyName)
            throws IOException
    {
        try {
            EncryptedKeyVersion encryptedKeyVersion = this.keyProvider.generateEncryptedKey(keyName);
            // Decrypt working key right away so we can write data.
            byte[] plainTextWorkingKey = this.keyProvider.decryptEncryptedKey(encryptedKeyVersion).getMaterial();
            // Get key metadata to save it in parquet footer.
            byte[] encryptedWorkingKey = encryptedKeyVersion.getEncryptedKeyVersion().getMaterial();
            String versionName = encryptedKeyVersion.getEncryptionKeyVersionName();
            String[] parts = versionName.split("@");
            if (parts.length != 2) {
                throw new InvalidObjectException("Malformed version name," +
                        " version name must follow format of name and version number separated by '@'");
            }
            String masterKeyName = parts[0];
            int masterKeyVersion = Integer.parseInt(parts[1]);
            byte[] iv = encryptedKeyVersion.getEncryptedKeyIv();
            byte[] keyMetadata = KeyMetadataAssembler.assembly(masterKeyName, iv, masterKeyVersion, encryptedWorkingKey);
            return new EKeySet(plainTextWorkingKey, keyMetadata);
        }
        catch (GeneralSecurityException se) {
            throw new IOException(se);
        }
        catch (Exception e) {
            throw e;
        }
    }
}
