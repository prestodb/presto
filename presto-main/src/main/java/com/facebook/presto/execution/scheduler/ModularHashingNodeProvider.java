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
package com.facebook.presto.execution.scheduler;

import com.facebook.presto.metadata.InternalNode;
import com.facebook.presto.spi.HostAddress;
import com.facebook.presto.spi.PrestoException;
import com.google.common.hash.HashFunction;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.common.type.encoding.StringUtils.UTF_8;
import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static com.google.common.hash.Hashing.murmur3_32;
import static java.lang.Math.toIntExact;
import static java.util.Collections.unmodifiableList;

public class ModularHashingNodeProvider
{
    private static final HashFunction HASH_FUNCTION = murmur3_32();

    private final List<InternalNode> sortedCandidates;

    public ModularHashingNodeProvider(List<InternalNode> sortedCandidates)
    {
        if (sortedCandidates == null || sortedCandidates.isEmpty()) {
            throw new PrestoException(NO_NODES_AVAILABLE, "sortedCandidates is null or empty for ModularHashingNodeProvider");
        }
        this.sortedCandidates = sortedCandidates;
    }

    public List<HostAddress> get(String identifier, int count)
    {
        int size = sortedCandidates.size();
        int position = toIntExact((HASH_FUNCTION.hashString(identifier, UTF_8).asInt() & 0xFFFFFFFFL) % size);
        List<HostAddress> chosenCandidates = new ArrayList<>();
        if (count > size) {
            count = size;
        }
        for (int i = 0; i < count && i < sortedCandidates.size(); i++) {
            chosenCandidates.add(sortedCandidates.get((position + i) % size).getHostAndPort());
        }
        return unmodifiableList(chosenCandidates);
    }
}
