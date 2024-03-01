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
import com.facebook.presto.hive.metastore.Table;
import com.facebook.presto.spi.ConnectorSession;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.facebook.presto.hive.DwrfEncryptionMetadata.forPerField;
import static com.facebook.presto.hive.DwrfEncryptionMetadata.forTable;
import static com.facebook.presto.hive.EncryptionInformation.fromEncryptionMetadata;
import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.function.Function.identity;

class TestDwrfEncryptionInformationSource
        extends AbstractDwrfEncryptionInformationSource
{
    public static final String TEST_EXTRA_METADATA = "test.extra.metadata";

    @Override
    protected Map<String, EncryptionInformation> getReadEncryptionInformationInternal(
            ConnectorSession session,
            Table table,
            Optional<Set<HiveColumnHandle>> requestedColumns,
            Map<String, Partition> partitions,
            Map<String, String> fieldToKeyReference,
            DwrfTableEncryptionProperties encryptionProperties)
    {
        Map<String, byte[]> fieldToKeyData = fieldToKeyReference.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getBytes()));
        return partitions.keySet().stream()
                .collect(toImmutableMap(identity(), partitionName -> fromEncryptionMetadata(
                        forPerField(
                                fieldToKeyData,
                                ImmutableMap.of(TEST_EXTRA_METADATA, partitionName),
                                encryptionProperties.getEncryptionAlgorithm(),
                                encryptionProperties.getEncryptionProvider()))));
    }

    @Override
    protected EncryptionInformation getReadEncryptionInformationInternal(
            ConnectorSession session,
            Table table,
            Optional<Set<HiveColumnHandle>> requestedColumns,
            Map<String, String> fieldToKeyReference,
            DwrfTableEncryptionProperties encryptionProperties)
    {
        Map<String, byte[]> fieldToKeyData = fieldToKeyReference.entrySet().stream()
                .collect(toImmutableMap(Map.Entry::getKey, entry -> entry.getValue().getBytes()));
        return fromEncryptionMetadata(
                        forPerField(
                                fieldToKeyData,
                                ImmutableMap.of(TEST_EXTRA_METADATA, table.getTableName()),
                                encryptionProperties.getEncryptionAlgorithm(),
                                encryptionProperties.getEncryptionProvider()));
    }

    @Override
    protected EncryptionInformation getWriteEncryptionInformationInternal(
            ConnectorSession session,
            DwrfTableEncryptionProperties tableEncryptionProperties,
            String dbName,
            String tableName)
    {
        if (tableEncryptionProperties.getEncryptTable().isPresent()) {
            return fromEncryptionMetadata(
                    forTable(
                            tableEncryptionProperties.getEncryptTable().get().getBytes(),
                            ImmutableMap.of(TEST_EXTRA_METADATA, tableEncryptionProperties.getEncryptionAlgorithm()),
                            tableEncryptionProperties.getEncryptionAlgorithm(),
                            tableEncryptionProperties.getEncryptionProvider()));
        }
        else if (tableEncryptionProperties.getColumnEncryptionInformation().isPresent()) {
            return fromEncryptionMetadata(
                    forPerField(
                            tableEncryptionProperties.getColumnEncryptionInformation().get().getColumnToKeyReference().entrySet().stream()
                                    .collect(toImmutableMap(entry -> entry.getKey().toString(), entry -> entry.getValue().getBytes())),
                            ImmutableMap.of(TEST_EXTRA_METADATA, tableEncryptionProperties.getEncryptionAlgorithm()),
                            tableEncryptionProperties.getEncryptionAlgorithm(),
                            tableEncryptionProperties.getEncryptionProvider()));
        }

        throw new IllegalStateException("One of table or column property should have a non-empty value");
    }
}
