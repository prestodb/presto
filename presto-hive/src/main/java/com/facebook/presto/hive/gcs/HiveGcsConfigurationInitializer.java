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
package com.facebook.presto.hive.gcs;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.common.security.CryptoLibrary;
import com.facebook.presto.spi.PrestoException;
import com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystem;
import jakarta.inject.Inject;
import org.apache.hadoop.conf.Configuration;

import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Paths;
import java.util.Base64;

import static com.facebook.presto.common.util.LocalFileWriter.writeTextToFile;
import static com.facebook.presto.spi.StandardErrorCode.GENERIC_INTERNAL_ERROR;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemBase.AUTHENTICATION_PREFIX;
import static com.google.cloud.hadoop.fs.gcs.GoogleHadoopFileSystemConfiguration.AUTH_SERVICE_ACCOUNT_ENABLE;
import static com.google.cloud.hadoop.util.AccessTokenProviderClassFromConfigFactory.ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX;
import static com.google.cloud.hadoop.util.EntriesCredentialConfiguration.JSON_KEYFILE_SUFFIX;
import static com.google.common.base.Preconditions.checkArgument;

public class HiveGcsConfigurationInitializer
        implements GcsConfigurationInitializer
{
    private static final Logger log = Logger.get(HiveGcsConfigurationInitializer.class);
    private static final int BUFFER_SIZE = 4096;
    private final boolean useGcsAccessToken;
    private final String jsonKeyFilePath;

    @Inject
    public HiveGcsConfigurationInitializer(HiveGcsConfig config)
    {
        this.useGcsAccessToken = config.isUseGcsAccessToken();
        this.jsonKeyFilePath = config.getJsonKeyFilePath();
    }

    public void updateConfiguration(Configuration config)
    {
        config.set("fs.gs.impl", GoogleHadoopFileSystem.class.getName());

        if (useGcsAccessToken) {
            // use oauth token to authenticate with Google Cloud Storage
            config.set(AUTH_SERVICE_ACCOUNT_ENABLE.getKey(), "false");
            config.set(AUTHENTICATION_PREFIX + ACCESS_TOKEN_PROVIDER_IMPL_SUFFIX, GcsAccessTokenProvider.class.getName());
        }
        else if (jsonKeyFilePath != null) {
            // use service account key file
            config.set(AUTH_SERVICE_ACCOUNT_ENABLE.getKey(), "true");

            String encryptedJsonKey = readFileContents(jsonKeyFilePath);
            String decryptedJsonKeyFilePath = constructDecryptedFilePath(jsonKeyFilePath);
            decryptProperties(encryptedJsonKey, decryptedJsonKeyFilePath);
            config.set(AUTHENTICATION_PREFIX + JSON_KEYFILE_SUFFIX, decryptedJsonKeyFilePath);
        }
    }

    private String constructDecryptedFilePath(String jsonKeyFilePath)
    {
        return jsonKeyFilePath.substring(0, jsonKeyFilePath.lastIndexOf('/') + 1) + "decryptedFile.json";
    }

    private void decryptProperties(String encryptedString, String decryptedJsonKeyFilePath)
    {
        // Decrypt the encrypted JSON string and then decode it to retrieve the Key.
        try {
            final int outPlaintextLength = 2 * BUFFER_SIZE;
            byte[] outPlaintext = new byte[outPlaintextLength];
            int decResp = CryptoLibrary.INSTANCE.do_decrypt_string(encryptedString, outPlaintext, outPlaintextLength);
            byte[] decodedBytes = Base64.getDecoder().decode(new String(outPlaintext, 0, decResp));
            String decryptedKey = new String(decodedBytes, StandardCharsets.UTF_8);
            writeTextToFile(decryptedKey, decryptedJsonKeyFilePath);
        }
        catch (Exception e) {
            log.error("Error occurred while decrypting json string", e);
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unable to decrypt JsonKeyfile");
        }
    }

    private String readFileContents(String fileLocation)
    {
        //Read file content in encrypted file.
        checkArgument(fileLocation != null && !fileLocation.isEmpty(), "File path is null or empty");
        try {
            byte[] bytes = Files.readAllBytes(Paths.get(fileLocation));
            return new String(bytes, StandardCharsets.UTF_8);
        }
        catch (Exception exc) {
            log.error("Error occurred while loading encrypted properties from jsonKey file");
            throw new PrestoException(GENERIC_INTERNAL_ERROR, "Unable to load encrypted file properties");
        }
    }
}
