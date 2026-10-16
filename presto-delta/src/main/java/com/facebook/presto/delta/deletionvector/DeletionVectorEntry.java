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
package com.facebook.presto.delta.deletionvector;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class DeletionVectorEntry
{
    private final String storageType;
    private final String pathOrInlineDv;
    private final Optional<Integer> offset;
    private final int sizeInBytes;
    private final long cardinality;

    @JsonCreator
    public DeletionVectorEntry(
            @JsonProperty("storageType") String storageType,
            @JsonProperty("pathOrInlineDv") String pathOrInlineDv,
            @JsonProperty("offset") Optional<Integer> offset,
            @JsonProperty("sizeInBytes") int sizeInBytes,
            @JsonProperty("cardinality") long cardinality)
    {
        requireNonNull(storageType, "storageType is null");
        requireNonNull(pathOrInlineDv, "pathOrInlineDv is null");
        requireNonNull(offset, "offset is null");
        this.storageType = storageType;
        this.pathOrInlineDv = pathOrInlineDv;
        this.offset = offset;
        this.sizeInBytes = sizeInBytes;
        this.cardinality = cardinality;
    }

    @JsonProperty
    public String getStorageType()
    {
        return storageType;
    }

    @JsonProperty
    public String getPathOrInlineDv()
    {
        return pathOrInlineDv;
    }

    @JsonProperty
    public Optional<Integer> getOffset()
    {
        return offset;
    }

    @JsonProperty
    public int sizeInBytes()
    {
        return sizeInBytes;
    }

    @JsonProperty
    public long getCardinality()
    {
        return cardinality;
    }
}
