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

import com.facebook.presto.orc.proto.DwrfProto.KeyInfo;
import com.google.common.collect.ImmutableMap;
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Map;
import java.util.Set;

import static java.util.Objects.requireNonNull;

public class DwrfStreamOrderingConfig
{
    // column is the logical columns index for a table with n columns
    // This index range from 0 to n - 1
    private final ImmutableMap<Integer, Set<KeyInfo>> columnToKeySet;

    public DwrfStreamOrderingConfig(Map<Integer, List<KeyInfo>> columnToKeys)
    {
        requireNonNull(columnToKeys, "columnToKeys cannot be null");
        ImmutableMap.Builder<Integer, Set<KeyInfo>> columnToKeySetBuilder = new ImmutableMap.Builder<>();
        for (Map.Entry<Integer, List<KeyInfo>> entry : columnToKeys.entrySet()) {
            columnToKeySetBuilder.put(entry.getKey(), ImmutableSet.copyOf(entry.getValue()));
        }
        columnToKeySet = columnToKeySetBuilder.build();
    }

    public Map<Integer, Set<KeyInfo>> getStreamOrdering()
    {
        return columnToKeySet;
    }
}
