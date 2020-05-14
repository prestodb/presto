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

import com.facebook.presto.orc.metadata.DwrfEncryption;
import com.facebook.presto.orc.metadata.EncryptionGroup;
import com.facebook.presto.orc.metadata.KeyProvider;
import com.facebook.presto.orc.metadata.OrcType;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;

import static java.util.Objects.requireNonNull;

public class DwrfDecryptorProvider
{
    private final Map<Integer, Integer> nodeToGroupMap;
    private final EncryptionLibrary encryptionLibrary;

    private DwrfDecryptorProvider(EncryptionLibrary encryptionLibrary, Map<Integer, Integer> nodeToGroupMap)
    {
        this.encryptionLibrary = requireNonNull(encryptionLibrary, "encryption is null");
        this.nodeToGroupMap = requireNonNull(nodeToGroupMap, "nodeToGroupMap is null");
    }

    public static DwrfDecryptorProvider createDwrfDecryptorProvider(DwrfEncryption dwrfEncryption, List<OrcType> types)
    {
        return new DwrfDecryptorProvider(
                getDecryptionLibrary(dwrfEncryption.getKeyProvider()),
                createNodeToGroupMap(dwrfEncryption.getEncryptionGroups(), types));
    }

    @VisibleForTesting
    public static Map<Integer, Integer> createNodeToGroupMap(List<EncryptionGroup> encryptionGroups, List<OrcType> types)
    {
        ImmutableMap.Builder<Integer, Integer> nodeToGroupMapBuilder = ImmutableMap.builder();
        for (int groupId = 0; groupId < encryptionGroups.size(); groupId++) {
            // if group has key, use that.  otherwise use from stripe
            for (Integer rootId : encryptionGroups.get(groupId).getNodes()) {
                OrcType type = types.get(rootId);
                for (int childId = rootId; childId < rootId + type.getFieldCount() + 1; childId++) {
                    nodeToGroupMapBuilder.put(childId, groupId);
                }
            }
        }
        return nodeToGroupMapBuilder.build();
    }

    private static EncryptionLibrary getDecryptionLibrary(KeyProvider keyProvider)
    {
        switch (keyProvider) {
            case CRYPTO_SERVICE:
                return getCryptoServiceDecryptor();
            default:
                return getUnknownDecryptor();
        }
    }

    private static EncryptionLibrary getCryptoServiceDecryptor()
    {
        // TODO: configure and pass through
        return new TestingEncryptionLibrary();
    }

    private static EncryptionLibrary getUnknownDecryptor()
    {
        // TODO: configure default decryptor and pass through
        return new TestingEncryptionLibrary();
    }

    public DwrfDecryptor createDecryptor(Slice keyMetadata)
    {
        return new DwrfDecryptor(keyMetadata, encryptionLibrary);
    }

    public Map<Integer, Integer> getNodeToGroupMap()
    {
        return nodeToGroupMap;
    }
}
