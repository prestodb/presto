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

import com.facebook.presto.hive.metastore.Partition;
import com.facebook.presto.hive.metastore.Storage;
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.HiveTestUtils.SESSION;
import static com.facebook.presto.hive.metastore.PrestoTableType.MANAGED_TABLE;
import static com.facebook.presto.hive.metastore.StorageFormat.VIEW_STORAGE_FORMAT;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;
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

        assertFalse(provider.getReadEncryptionInformation(SESSION, TEST_TABLE, Optional.empty()).isPresent());
    }

    @Test
    public void testReturnsFirstNonEmptyObject()
    {
        EncryptionInformation encryptionInformation1 = TestEncryptionInformationSource.createEncryptionInformation("test1");
        EncryptionInformation encryptionInformation2 = TestEncryptionInformationSource.createEncryptionInformation("test2");

        HiveEncryptionInformationProvider provider = new HiveEncryptionInformationProvider(ImmutableList.of(
                new TestEncryptionInformationSource(Optional.empty()),
                new TestEncryptionInformationSource(Optional.empty()),
                new TestEncryptionInformationSource(Optional.of(encryptionInformation1)),
                new TestEncryptionInformationSource(Optional.of(encryptionInformation2))));

        assertEquals(provider.getReadEncryptionInformation(SESSION, TEST_TABLE, Optional.empty()).get(), encryptionInformation1);
    }

    private static final class TestEncryptionInformationSource
            implements EncryptionInformationSource
    {
        private final Optional<EncryptionInformation> encryptionInformation;

        public TestEncryptionInformationSource(Optional<EncryptionInformation> encryptionInformation)
        {
            this.encryptionInformation = requireNonNull(encryptionInformation, "encryptionInformation is null");
        }

        @Override
        public Optional<Map<String, EncryptionInformation>> getReadEncryptionInformation(ConnectorSession session, Table table, Optional<Set<HiveColumnHandle>> requestedColumns, Map<String, Partition> partitions)
        {
            return encryptionInformation.map(information -> partitions.keySet().stream().collect(toImmutableMap(identity(), partitionId -> information)));
        }

        @Override
        public Optional<EncryptionInformation> getReadEncryptionInformation(ConnectorSession session, Table table, Optional<Set<HiveColumnHandle>> requestedColumns)
        {
            return encryptionInformation;
        }

        @Override
        public Optional<EncryptionInformation> getWriteEncryptionInformation(ConnectorSession session, TableEncryptionProperties tableEncryptionProperties, String dbName, String tableName)
        {
            return encryptionInformation;
        }

        public static EncryptionInformation createEncryptionInformation(String fieldName)
        {
            return EncryptionInformation.fromEncryptionMetadata(DwrfEncryptionMetadata.forPerField(ImmutableMap.of(fieldName, fieldName.getBytes()), ImmutableMap.of(), "algo1", "provider1"));
        }
    }
}
