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

import io.airlift.slice.Slice;

import java.util.List;

import static java.util.Objects.requireNonNull;

public class WriterEncryptionGroup
{
    // Sub trees in the schema that encryption should be applied
    // Sub tree is identified by its root id
    private final List<Integer> nodes;

    // key metadata for encrypting the dataKeyMetadata
    private final Slice intermediateKeyMetadata;

    public WriterEncryptionGroup(List<Integer> nodes, Slice intermediateKeyMetadata)
    {
        this.nodes = requireNonNull(nodes, "nodes is null");
        this.intermediateKeyMetadata = requireNonNull(intermediateKeyMetadata, "intermediateKeyMetadata is null");
    }

    public List<Integer> getNodes()
    {
        return nodes;
    }

    public Slice getIntermediateKeyMetadata()
    {
        return intermediateKeyMetadata;
    }
}
