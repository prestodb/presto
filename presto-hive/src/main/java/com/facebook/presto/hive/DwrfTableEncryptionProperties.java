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

import com.facebook.presto.spi.PrestoException;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.ColumnEncryptionInformation.fromHiveProperty;
import static com.facebook.presto.hive.EncryptionProperties.DWRF_ENCRYPTION_ALGORITHM_KEY;
import static com.facebook.presto.hive.EncryptionProperties.DWRF_ENCRYPTION_PROVIDER_KEY;
import static com.facebook.presto.hive.EncryptionProperties.ENCRYPT_COLUMNS_KEY;
import static com.facebook.presto.hive.EncryptionProperties.ENCRYPT_TABLE_KEY;
import static com.facebook.presto.hive.HiveErrorCode.HIVE_INVALID_ENCRYPTION_METADATA;
import static com.facebook.presto.hive.HiveTableProperties.DWRF_ENCRYPTION_ALGORITHM;
import static com.facebook.presto.hive.HiveTableProperties.DWRF_ENCRYPTION_PROVIDER;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DwrfTableEncryptionProperties
        extends TableEncryptionProperties
{
    private final String encryptionAlgorithm;
    private final String encryptionProvider;

    private DwrfTableEncryptionProperties(
            Optional<String> encryptTable,
            Optional<ColumnEncryptionInformation> columnEncryptionInformation,
            String encryptionAlgorithm,
            String encryptionProvider)
    {
        super(encryptTable, columnEncryptionInformation);
        this.encryptionAlgorithm = requireNonNull(encryptionAlgorithm, "encryptionAlgorithm is null");
        this.encryptionProvider = requireNonNull(encryptionProvider, "encryptionProvider is null");
    }

    public String getEncryptionAlgorithm()
    {
        return encryptionAlgorithm;
    }

    public String getEncryptionProvider()
    {
        return encryptionProvider;
    }

    @Override
    protected Map<String, String> getFormatSpecificHiveProperties()
    {
        return ImmutableMap.of(
                DWRF_ENCRYPTION_ALGORITHM_KEY, encryptionAlgorithm,
                DWRF_ENCRYPTION_PROVIDER_KEY, encryptionProvider);
    }

    @Override
    protected Map<String, Object> getFormatSpecificTableProperties()
    {
        return ImmutableMap.of(
                DWRF_ENCRYPTION_ALGORITHM, encryptionAlgorithm,
                DWRF_ENCRYPTION_PROVIDER, encryptionProvider);
    }

    public static DwrfTableEncryptionProperties forTable(String encryptTable, String encryptionAlgorithm, String encryptionProvider)
    {
        return new DwrfTableEncryptionProperties(Optional.of(encryptTable), Optional.empty(), encryptionAlgorithm, encryptionProvider);
    }

    public static DwrfTableEncryptionProperties forPerColumn(ColumnEncryptionInformation columnEncryptionInformation, String encryptionAlgorithm, String encryptionProvider)
    {
        return new DwrfTableEncryptionProperties(Optional.empty(), Optional.of(columnEncryptionInformation), encryptionAlgorithm, encryptionProvider);
    }

    public static Optional<DwrfTableEncryptionProperties> fromHiveTableProperties(Map<String, String> properties)
    {
        String encryptTable = properties.get(ENCRYPT_TABLE_KEY);
        String encryptColumns = properties.get(ENCRYPT_COLUMNS_KEY);

        if (encryptTable != null || encryptColumns != null) {
            if (!properties.containsKey(DWRF_ENCRYPTION_ALGORITHM_KEY) || !properties.containsKey(DWRF_ENCRYPTION_PROVIDER_KEY)) {
                throw new PrestoException(HIVE_INVALID_ENCRYPTION_METADATA, format("Both %s and %s need to be set for DWRF encryption", DWRF_ENCRYPTION_ALGORITHM_KEY, DWRF_ENCRYPTION_PROVIDER_KEY));
            }

            if (encryptTable != null) {
                return Optional.of(forTable(encryptTable, properties.get(DWRF_ENCRYPTION_ALGORITHM_KEY), properties.get(DWRF_ENCRYPTION_PROVIDER_KEY)));
            }

            return Optional.of(forPerColumn(
                    fromHiveProperty(encryptColumns),
                    properties.get(DWRF_ENCRYPTION_ALGORITHM_KEY),
                    properties.get(DWRF_ENCRYPTION_PROVIDER_KEY)));
        }

        return Optional.empty();
    }
}
