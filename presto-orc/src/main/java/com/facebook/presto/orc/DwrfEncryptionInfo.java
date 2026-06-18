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

import com.facebook.presto.orc.metadata.OrcType;
import com.google.common.base.VerifyException;
import com.google.common.collect.ImmutableList;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Optional;
import java.util.Set;

import static com.google.common.base.Verify.verify;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DwrfEncryptionInfo
{
    public static final DwrfEncryptionInfo UNENCRYPTED = new DwrfEncryptionInfo(ImmutableMap.of(), ImmutableList.of(), ImmutableMap.of());
    private final Map<Integer, DwrfDataEncryptor> dwrfEncryptors;
    private final List<byte[]> encryptedKeyMetadatas;
    private final Map<Integer, Integer> nodeToGroupMap;

    public DwrfEncryptionInfo(Map<Integer, DwrfDataEncryptor> dwrfEncryptors, List<byte[]> encryptedKeyMetadatas, Map<Integer, Integer> nodeToGroupMap)
    {
        this.dwrfEncryptors = ImmutableMap.copyOf(requireNonNull(dwrfEncryptors, "dwrfDecryptors is null"));
        this.encryptedKeyMetadatas = ImmutableList.copyOf(requireNonNull(encryptedKeyMetadatas, "keyMetadatas is null"));
        this.nodeToGroupMap = ImmutableMap.copyOf(requireNonNull(nodeToGroupMap, "nodeToGroupMap is null"));
    }

    public static DwrfEncryptionInfo createDwrfEncryptionInfo(
            EncryptionLibrary encryptionLibrary,
            List<byte[]> encryptedKeyMetadatas,
            Map<Integer, Slice> intermediateKeyMetadatas,
            Map<Integer, Integer> nodeToGroupMap)
    {
        // A user might only have permission to read columns from some encryption groups
        // create encryptors for the groups a user has IEKs for
        ImmutableMap.Builder<Integer, DwrfDataEncryptor> encryptorsBuilder = ImmutableMap.builder();
        for (Integer groupId : intermediateKeyMetadatas.keySet()) {
            byte[] encryptedDataKey = encryptedKeyMetadatas.get(groupId);
            byte[] decryptedKeyMetadata = encryptionLibrary.decryptKey(intermediateKeyMetadatas.get(groupId).getBytes(), encryptedDataKey, 0, encryptedDataKey.length);
            encryptorsBuilder.put(groupId, new DwrfDataEncryptor(decryptedKeyMetadata, encryptionLibrary));
        }

        return new DwrfEncryptionInfo(encryptorsBuilder.build(), encryptedKeyMetadatas, nodeToGroupMap);
    }

    public static Map<Integer, Integer> createNodeToGroupMap(List<List<Integer>> encryptionGroups, List<OrcType> types)
    {
        // We don't use an immutableMap builder so that we can check what's already been added
        Map<Integer, Integer> nodeToGroupMapBuilder = new HashMap();
        for (int groupId = 0; groupId < encryptionGroups.size(); groupId++) {
            for (Integer nodeId : encryptionGroups.get(groupId)) {
                fillNodeToGroupMap(groupId, nodeId, types, nodeToGroupMapBuilder);
            }
        }
        return ImmutableMap.copyOf(nodeToGroupMapBuilder);
    }

    private static void fillNodeToGroupMap(int groupId, int nodeId, List<OrcType> types, Map<Integer, Integer> nodeToGroupMapBuilder)
    {
        if (nodeToGroupMapBuilder.containsKey(nodeId) && nodeToGroupMapBuilder.get(nodeId) != groupId) {
            throw new VerifyException(format("Column or sub-column %s belongs to more than one encryption group: %s and %s", nodeId, nodeToGroupMapBuilder.get(nodeId), groupId));
        }
        nodeToGroupMapBuilder.put(nodeId, groupId);
        OrcType type = types.get(nodeId);
        for (int childIndex : type.getFieldTypeIndexes()) {
            fillNodeToGroupMap(groupId, childIndex, types, nodeToGroupMapBuilder);
        }
    }

    public DwrfDataEncryptor getEncryptorByGroupId(int groupId)
    {
        verify(dwrfEncryptors.containsKey(groupId), "no encryptor available for group %s", groupId);
        return dwrfEncryptors.get(groupId);
    }

    public Optional<DwrfDataEncryptor> getEncryptorByNodeId(int nodeId)
    {
        if (!nodeToGroupMap.containsKey(nodeId)) {
            return Optional.empty();
        }
        return Optional.of(getEncryptorByGroupId(nodeToGroupMap.get(nodeId)));
    }

    public Optional<Integer> getGroupByNodeId(int nodeId)
    {
        return Optional.ofNullable(nodeToGroupMap.get(nodeId));
    }

    public Set<Integer> getEncryptorGroupIds()
    {
        return dwrfEncryptors.keySet();
    }

    public List<byte[]> getEncryptedKeyMetadatas()
    {
        return encryptedKeyMetadatas;
    }
}
