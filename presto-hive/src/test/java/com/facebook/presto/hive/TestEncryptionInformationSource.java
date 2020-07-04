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

import static com.google.common.collect.ImmutableMap.toImmutableMap;
import static java.util.Objects.requireNonNull;
import static java.util.function.Function.identity;

class TestEncryptionInformationSource
        implements EncryptionInformationSource
{
    private Optional<EncryptionInformation> encryptionInformation;

    TestEncryptionInformationSource(Optional<EncryptionInformation> encryptionInformation)
    {
        this.encryptionInformation = requireNonNull(encryptionInformation, "encryptionInformation is null");
    }

    @Override
    public Optional<Map<String, EncryptionInformation>> getEncryptionInformation(ConnectorSession session, Table table, Optional<Set<HiveColumnHandle>> requestedColumns, Map<String, Partition> partitions)
    {
        return encryptionInformation.map(information -> partitions.keySet().stream().collect(toImmutableMap(identity(), partitionId -> information)));
    }

    @Override
    public Optional<EncryptionInformation> getEncryptionInformation(ConnectorSession session, Table table, Optional<Set<HiveColumnHandle>> requestedColumns)
    {
        return encryptionInformation;
    }

    public static EncryptionInformation createEncryptionInformation(String fieldName)
    {
        return EncryptionInformation.fromEncryptionMetadata(new DwrfEncryptionMetadata(ImmutableMap.of(fieldName, fieldName.getBytes())));
    }
}
