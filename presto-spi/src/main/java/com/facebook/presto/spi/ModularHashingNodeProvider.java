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
package com.facebook.presto.spi;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.spi.StandardErrorCode.NO_NODES_AVAILABLE;
import static java.util.Collections.unmodifiableList;

public class ModularHashingNodeProvider<T>
        implements NodeProvider<T>
{
    private final List<T> sortedCandidates;

    public ModularHashingNodeProvider(List<T> sortedCandidates)
    {
        if (sortedCandidates == null || sortedCandidates.isEmpty()) {
            throw new PrestoException(NO_NODES_AVAILABLE, "sortedCandidates is null or empty for ModularHashingNodeProvider");
        }
        this.sortedCandidates = sortedCandidates;
    }

    /**
     * Provider function that returns a list of nodes for a given key, internally it will use modular hashing function
     * Use key's hashcode to mode node candidates size as the target position and get the corresponding node from the list
     * If target count is larger than one, sequentially get the rest of the nodes following the target node
     *
     */
    @Override
    public List<T> get(Object key, int count)
    {
        int size = sortedCandidates.size();
        int mod = key.hashCode() % size;
        int position = mod < 0 ? mod + size : mod;
        List<T> chosenCandidates = new ArrayList<>();
        for (int i = 0; i < count && i < sortedCandidates.size(); i++) {
            chosenCandidates.add(sortedCandidates.get((position + i) % size));
        }
        return unmodifiableList(chosenCandidates);
    }
}
