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
package com.facebook.presto.lance;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;

import java.util.List;

import static java.util.Objects.requireNonNull;

/**
 * Per-worker result of a row-level DELETE/UPDATE/MERGE, serialized from the
 * {@link com.facebook.presto.spi.UpdatablePageSink}/page source to the coordinator.
 * Carries the rows to delete (grouped by fragment), the new fragments written for
 * inserted/updated rows, and the dataset version the operation read from.
 */
public class LanceMergeCommitData
{
    private final List<FragmentDeletion> deletions;
    private final String newFragmentsJson;
    private final long readVersion;
    private final long writtenBytes;
    private final long rowCount;

    @JsonCreator
    public LanceMergeCommitData(
            @JsonProperty("deletions") List<FragmentDeletion> deletions,
            @JsonProperty("newFragmentsJson") String newFragmentsJson,
            @JsonProperty("readVersion") long readVersion,
            @JsonProperty("writtenBytes") long writtenBytes,
            @JsonProperty("rowCount") long rowCount)
    {
        this.deletions = ImmutableList.copyOf(requireNonNull(deletions, "deletions is null"));
        this.newFragmentsJson = requireNonNull(newFragmentsJson, "newFragmentsJson is null");
        this.readVersion = readVersion;
        this.writtenBytes = writtenBytes;
        this.rowCount = rowCount;
    }

    @JsonProperty
    public List<FragmentDeletion> getDeletions()
    {
        return deletions;
    }

    @JsonProperty
    public String getNewFragmentsJson()
    {
        return newFragmentsJson;
    }

    @JsonProperty
    public long getReadVersion()
    {
        return readVersion;
    }

    @JsonProperty
    public long getWrittenBytes()
    {
        return writtenBytes;
    }

    @JsonProperty
    public long getRowCount()
    {
        return rowCount;
    }
}
