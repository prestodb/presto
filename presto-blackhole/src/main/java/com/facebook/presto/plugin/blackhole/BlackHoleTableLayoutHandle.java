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
package com.facebook.presto.plugin.blackhole;

import com.facebook.presto.spi.ConnectorTableLayoutHandle;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import io.airlift.units.Duration;

import java.util.Objects;

public final class BlackHoleTableLayoutHandle
        implements ConnectorTableLayoutHandle
{
    private final int splitCount;
    private final int pagesPerSplit;
    private final int rowsPerPage;
    private final int fieldsLength;
    private final Duration pageProcessingDelay;

    @JsonCreator
    public BlackHoleTableLayoutHandle(
            @JsonProperty("splitCount") int splitCount,
            @JsonProperty("pagesPerSplit") int pagesPerSplit,
            @JsonProperty("rowsPerPage") int rowsPerPage,
            @JsonProperty("fieldsLength") int fieldsLength,
            @JsonProperty("pageProcessingDelay") Duration pageProcessingDelay)
    {
        this.splitCount = splitCount;
        this.pagesPerSplit = pagesPerSplit;
        this.rowsPerPage = rowsPerPage;
        this.fieldsLength = fieldsLength;
        this.pageProcessingDelay = pageProcessingDelay;
    }

    @JsonProperty
    public int getSplitCount()
    {
        return splitCount;
    }

    @JsonProperty
    public int getPagesPerSplit()
    {
        return pagesPerSplit;
    }

    @JsonProperty
    public int getRowsPerPage()
    {
        return rowsPerPage;
    }

    @JsonProperty
    public int getFieldsLength()
    {
        return fieldsLength;
    }

    @JsonProperty
    public Duration getPageProcessingDelay()
    {
        return pageProcessingDelay;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getSplitCount(), getPagesPerSplit(), getRowsPerPage());
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if (obj == null || getClass() != obj.getClass()) {
            return false;
        }
        BlackHoleTableLayoutHandle other = (BlackHoleTableLayoutHandle) obj;
        return Objects.equals(this.getSplitCount(), other.getSplitCount()) &&
                Objects.equals(this.getPagesPerSplit(), other.getPagesPerSplit()) &&
                Objects.equals(this.getRowsPerPage(), other.getRowsPerPage());
    }
}
