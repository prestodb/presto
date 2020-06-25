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

import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.ColumnEncryptionInformation.fromMap;
import static com.facebook.presto.hive.DwrfTableEncryptionProperties.forPerColumn;
import static com.facebook.presto.hive.DwrfTableEncryptionProperties.forTable;
import static com.facebook.presto.hive.DwrfTableEncryptionProperties.fromHiveTableProperties;
import static com.facebook.presto.hive.EncryptionProperties.DWRF_ENCRYPTION_ALGORITHM_KEY;
import static com.facebook.presto.hive.EncryptionProperties.DWRF_ENCRYPTION_PROVIDER_KEY;
import static com.facebook.presto.hive.EncryptionProperties.ENCRYPT_COLUMNS_KEY;
import static com.facebook.presto.hive.EncryptionProperties.ENCRYPT_TABLE_KEY;
import static com.facebook.presto.hive.HiveTableProperties.DWRF_ENCRYPTION_ALGORITHM;
import static com.facebook.presto.hive.HiveTableProperties.DWRF_ENCRYPTION_PROVIDER;
import static com.facebook.presto.hive.HiveTableProperties.ENCRYPT_COLUMNS;
import static com.facebook.presto.hive.HiveTableProperties.ENCRYPT_TABLE;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;
import static org.testng.Assert.assertTrue;

public class TestDwrfTableEncryptionProperties
{
    @Test
    public void testEncryptTable()
    {
        DwrfTableEncryptionProperties properties = forTable("abcd", "test_algo", "test_prov");

        assertEquals(properties.toHiveProperties(), ImmutableMap.of(
                ENCRYPT_TABLE_KEY, "abcd",
                DWRF_ENCRYPTION_PROVIDER_KEY, "test_prov",
                DWRF_ENCRYPTION_ALGORITHM_KEY, "test_algo"));

        assertEquals(properties.toTableProperties(), ImmutableMap.of(
                ENCRYPT_TABLE, "abcd",
                DWRF_ENCRYPTION_PROVIDER, "test_prov",
                DWRF_ENCRYPTION_ALGORITHM, "test_algo"));
    }

    @Test
    public void testEncryptColumns()
    {
        ColumnEncryptionInformation columnEncryptionInformation = fromMap(ImmutableMap.of("c1", "abcd", "c2", "defg"));
        DwrfTableEncryptionProperties properties = forPerColumn(columnEncryptionInformation, "test_algo", "test_prov");

        assertEquals(properties.toHiveProperties(), ImmutableMap.of(
                ENCRYPT_COLUMNS_KEY, columnEncryptionInformation.toHiveProperty(),
                DWRF_ENCRYPTION_PROVIDER_KEY, "test_prov",
                DWRF_ENCRYPTION_ALGORITHM_KEY, "test_algo"));

        assertEquals(properties.toTableProperties(), ImmutableMap.of(
                ENCRYPT_COLUMNS, columnEncryptionInformation,
                DWRF_ENCRYPTION_PROVIDER, "test_prov",
                DWRF_ENCRYPTION_ALGORITHM, "test_algo"));
    }

    @Test
    public void testFromHiveTablePropertiesNonePresent()
    {
        Optional<DwrfTableEncryptionProperties> encryptionProperties = fromHiveTableProperties(ImmutableMap.of());
        assertFalse(encryptionProperties.isPresent());
    }

    @Test
    public void testFromHiveTablePropertiesTablePresent()
    {
        Map<String, String> hiveProperties = ImmutableMap.of(
                ENCRYPT_TABLE_KEY, "abcd",
                DWRF_ENCRYPTION_ALGORITHM_KEY, "test_algo",
                DWRF_ENCRYPTION_PROVIDER_KEY, "test_prov");
        Optional<DwrfTableEncryptionProperties> encryptionProperties = fromHiveTableProperties(hiveProperties);
        assertTrue(encryptionProperties.isPresent());
        assertEquals(encryptionProperties.get().toHiveProperties(), hiveProperties);
    }

    @Test
    public void testFromHiveTablePropertiesColumnPresent()
    {
        Map<String, String> hiveProperties = ImmutableMap.of(
                ENCRYPT_COLUMNS_KEY, "key1:col1",
                DWRF_ENCRYPTION_ALGORITHM_KEY, "test_algo",
                DWRF_ENCRYPTION_PROVIDER_KEY, "test_prov");
        Optional<DwrfTableEncryptionProperties> encryptionProperties = fromHiveTableProperties(hiveProperties);
        assertTrue(encryptionProperties.isPresent());
        assertEquals(encryptionProperties.get().toHiveProperties(), hiveProperties);
    }
}
