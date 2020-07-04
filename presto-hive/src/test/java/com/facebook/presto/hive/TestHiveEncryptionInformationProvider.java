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

import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Optional;

import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.TestEncryptionInformationSource.createEncryptionInformation;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertFalse;

public class TestHiveEncryptionInformationProvider
{
    private static final Table TEST_TABLE = new Table(
            "test_db",
            "test_table",
            "test_owner",
            MANAGED_TABLE,
            new Storage(
                    VIEW_STORAGE_FORMAT,
                    "",
                    Optional.empty(),
                    false,
                    ImmutableMap.of(),
                    ImmutableMap.of()),
            ImmutableList.of(),
            ImmutableList.of(),
            ImmutableMap.of(),
            Optional.empty(),
            Optional.empty());

    @Test
    public void testNoOneReturns()
    {
        HiveEncryptionInformationProvider provider = new HiveEncryptionInformationProvider(ImmutableList.of(
                new TestEncryptionInformationSource(Optional.empty()),
                new TestEncryptionInformationSource(Optional.empty())));

        assertFalse(provider.getEncryptionInformation(SESSION, TEST_TABLE, Optional.empty()).isPresent());
    }

    @Test
    public void testReturnsFirstNonEmptyObject()
    {
        EncryptionInformation encryptionInformation1 = createEncryptionInformation("test1");
        EncryptionInformation encryptionInformation2 = createEncryptionInformation("test2");

        HiveEncryptionInformationProvider provider = new HiveEncryptionInformationProvider(ImmutableList.of(
                new TestEncryptionInformationSource(Optional.empty()),
                new TestEncryptionInformationSource(Optional.empty()),
                new TestEncryptionInformationSource(Optional.of(encryptionInformation1)),
                new TestEncryptionInformationSource(Optional.of(encryptionInformation2))));

        assertEquals(provider.getEncryptionInformation(SESSION, TEST_TABLE, Optional.empty()).get(), encryptionInformation1);
    }
}
