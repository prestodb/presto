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
package com.facebook.presto.orc;

import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;
import java.util.Optional;

import static com.google.common.base.Verify.verify;
import static java.util.Objects.requireNonNull;

public class GroupDwrfDecryptors
{
    private final List<DwrfDecryptor> dwrfDecryptors;
    private final List<Slice> keyMetadatas;
    private final Map<Integer, Integer> nodeToGroupMap;

    public GroupDwrfDecryptors(List<DwrfDecryptor> dwrfDecryptors, List<Slice> keyMetadatas, Map<Integer, Integer> nodeToGroupMap)
    {
        this.dwrfDecryptors = ImmutableList.copyOf(requireNonNull(dwrfDecryptors, "dwrfDecryptors is null"));
        this.keyMetadatas = ImmutableList.copyOf(requireNonNull(keyMetadatas, "keyMetadatas is null"));
        this.nodeToGroupMap = ImmutableMap.copyOf(requireNonNull(nodeToGroupMap, "nodeToGroupMap is null"));
    }

    public static GroupDwrfDecryptors createGroupDwrfDecryptors(DwrfDecryptorProvider decryptorProvider, List<Slice> keyMetadatas)
    {
        ImmutableList.Builder<DwrfDecryptor> decryptorsBuilder = ImmutableList.builder();
        for (Slice keyMetadata : keyMetadatas) {
            decryptorsBuilder.add(decryptorProvider.createDecryptor(keyMetadata));
        }

        return new GroupDwrfDecryptors(decryptorsBuilder.build(), keyMetadatas, decryptorProvider.getNodeToGroupMap());
    }

    public DwrfDecryptor getDecryptorByGroupId(int groupId)
    {
        verify(groupId < dwrfDecryptors.size(), "groupId exceeds the size of dwrfDecryptors");
        return dwrfDecryptors.get(groupId);
    }

    public Optional<DwrfDecryptor> getDecryptorByNodeId(int nodeId)
    {
        if (!nodeToGroupMap.containsKey(nodeId)) {
            return Optional.empty();
        }
        return Optional.of(getDecryptorByGroupId(nodeToGroupMap.get(nodeId)));
    }

    public List<Slice> getKeyMetadatas()
    {
        return keyMetadatas;
    }
}
