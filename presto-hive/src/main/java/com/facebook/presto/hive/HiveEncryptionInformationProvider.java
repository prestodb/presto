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
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;

import javax.inject.Inject;

import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class HiveEncryptionInformationProvider
{
    private final List<EncryptionInformationSource> sources;

    @Inject
    public HiveEncryptionInformationProvider(Set<EncryptionInformationSource> sources)
    {
        this(ImmutableList.copyOf(requireNonNull(sources, "sources is null")));
    }

    @VisibleForTesting
    HiveEncryptionInformationProvider(List<EncryptionInformationSource> sources)
    {
        this.sources = ImmutableList.copyOf(requireNonNull(sources, "sources is null"));
    }

    public Optional<Map<String, EncryptionInformation>> getEncryptionInformation(
            ConnectorSession session,
            Table table,
            Optional<Set<HiveColumnHandle>> requestedColumns,
            Map<String, Partition> partitions)
    {
        for (EncryptionInformationSource source : sources) {
            Optional<Map<String, EncryptionInformation>> result = source.getEncryptionInformation(session, table, requestedColumns, partitions);
            if (result != null && result.isPresent()) {
                return result.map(ImmutableMap::copyOf);
            }
        }

        return Optional.empty();
    }

    public Optional<EncryptionInformation> getEncryptionInformation(
            ConnectorSession session,
            Table table,
            Optional<Set<HiveColumnHandle>> requestedColumns)
    {
        for (EncryptionInformationSource source : sources) {
            Optional<EncryptionInformation> result = source.getEncryptionInformation(session, table, requestedColumns);
            if (result != null && result.isPresent()) {
                return result;
            }
        }

        return Optional.empty();
    }
}
