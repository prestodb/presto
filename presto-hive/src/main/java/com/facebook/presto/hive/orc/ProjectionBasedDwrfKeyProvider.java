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
package com.facebook.presto.hive.orc;

import com.facebook.presto.hive.EncryptionInformation;
import com.facebook.presto.hive.HiveColumnHandle;
import com.facebook.presto.orc.DwrfKeyProvider;
import com.facebook.presto.orc.metadata.OrcType;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;
import org.apache.hadoop.fs.Path;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.facebook.presto.hive.HiveUtil.getPhysicalHiveColumnHandles;

/**
 * Builds the node to the intermediate encryption key map based on the requested Hive columns.
 */
public class ProjectionBasedDwrfKeyProvider
        implements DwrfKeyProvider
{
    private final List<HiveColumnHandle> columns;
    private final boolean useOrcColumnNames;
    private final Path path;
    private final Optional<EncryptionInformation> encryptionInformation;
    private Map<Integer, Slice> keys;

    public ProjectionBasedDwrfKeyProvider(Optional<EncryptionInformation> encryptionInformation, List<HiveColumnHandle> columns, boolean useOrcColumnNames, Path path)
    {
        this.encryptionInformation = encryptionInformation;
        this.columns = ImmutableList.copyOf(columns);
        this.useOrcColumnNames = useOrcColumnNames;
        this.path = path;
    }

    @Override
    public Map<Integer, Slice> getIntermediateKeys(List<OrcType> types)
    {
        if (keys == null) {
            if (encryptionInformation.isPresent() && encryptionInformation.get().getDwrfEncryptionMetadata().isPresent()) {
                List<HiveColumnHandle> physicalColumns = getPhysicalHiveColumnHandles(columns, useOrcColumnNames, types, path);
                keys = encryptionInformation.get().getDwrfEncryptionMetadata().get().toKeyMap(types, physicalColumns);
            }
            else {
                keys = ImmutableMap.of();
            }
        }

        return keys;
    }
}
