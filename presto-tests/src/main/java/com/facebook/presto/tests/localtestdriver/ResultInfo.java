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
package com.facebook.presto.tests.localtestdriver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.List;
import java.util.Objects;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ResultInfo
{
    private final Optional<String> updateType;
    private final Optional<Long> updateCount;
    private final List<Long> checkSums;
    private final long rowCount;

    @JsonCreator
    public ResultInfo(
            @JsonProperty("updateType") Optional<String> updateType,
            @JsonProperty("updateCount") Optional<Long> updateCount,
            @JsonProperty("checkSum") List<Long> checkSums,
            @JsonProperty("rowCount") long rowCount)
    {
        this.updateType = requireNonNull(updateType, "updateType is null");
        this.updateCount = requireNonNull(updateCount, "updateCount is null");
        this.checkSums = requireNonNull(checkSums, "checkSums is null");
        this.rowCount = rowCount;
    }

    @JsonProperty
    public Optional<String> getUpdateType()
    {
        return updateType;
    }

    @JsonProperty
    public Optional<Long> getUpdateCount()
    {
        return updateCount;
    }

    @JsonProperty
    public List<Long> getCheckSums()
    {
        return checkSums;
    }

    @JsonProperty
    public long getRowCount()
    {
        return rowCount;
    }

    @Override
    public boolean equals(Object other)
    {
        if (other == this) {
            return true;
        }
        if (!(other instanceof ResultInfo)) {
            return false;
        }
        ResultInfo that = (ResultInfo) other;
        return that.rowCount == rowCount && that.updateType.equals(updateType) && that.updateCount.equals(updateCount) && that.checkSums.equals(checkSums);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(updateType, updateCount, checkSums, rowCount);
    }
}
