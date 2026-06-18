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
import com.google.common.collect.ImmutableMap;
import io.airlift.slice.Slice;

import java.util.List;
import java.util.Map;

/**
 * Returns intermediate encryption keys in exchange for node types.
 */
public interface DwrfKeyProvider
{
    DwrfKeyProvider EMPTY = types -> ImmutableMap.of();

    /**
     * Construct a DwrfKeyProvider that always returns the same keys.
     */
    static DwrfKeyProvider of(Map<Integer, Slice> keys)
    {
        ImmutableMap<Integer, Slice> iek = ImmutableMap.copyOf(keys);
        return types -> iek;
    }

    /**
     * Get a node to the intermediate encryption key mapping.
     */
    Map<Integer, Slice> getIntermediateKeys(List<OrcType> types);
}
