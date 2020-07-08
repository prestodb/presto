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
package com.facebook.presto.orc.metadata;

import com.google.common.collect.ImmutableList;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class EncryptionGroup
{
    // Sub trees in the schema that encryption should be applied
    // Sub tree is identified by its root id
    private final List<Integer> nodes;
    // Arbitrary binary representing key metadata. It could be identifier
    // of key in KMS, encrypted DEK or other form of user defined key metadata.
    // this is optional and can use first keyMetadata from stripe
    private final Optional<Slice> keyMetadata;
    // encrypted columns stats (serialized FileStatistics)
    private final List<Slice> statistics;

    public EncryptionGroup(List<Integer> nodes, Optional<Slice> keyMetadata, List<Slice> statistics)
    {
        this.nodes = ImmutableList.copyOf(requireNonNull(nodes, "nodes is null"));
        this.keyMetadata = requireNonNull(keyMetadata, "keyMetadata is null");
        this.statistics = requireNonNull(statistics, "statistics is null");
    }

    public List<Integer> getNodes()
    {
        return nodes;
    }

    public Optional<Slice> getKeyMetadata()
    {
        return keyMetadata;
    }

    public List<Slice> getStatistics()
    {
        return statistics;
    }
}
