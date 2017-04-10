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

import com.facebook.presto.spi.ConnectorSplit;
import com.facebook.presto.spi.HostAddress;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.collect.ImmutableList;
import io.airlift.units.Duration;

import java.util.List;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public final class BlackHoleSplit
        implements ConnectorSplit
{
    private final int pagesCount;
    private final int rowsPerPage;
    private final int fieldsLength;
    private final Duration pageProcessingDelay;

    @JsonCreator
    public BlackHoleSplit(
            @JsonProperty("pagesCount") int pagesCount,
            @JsonProperty("rowsPerPage") int rowsPerPage,
            @JsonProperty("fieldsLength") int fieldsLength,
            @JsonProperty("pageProcessingDelay") Duration pageProcessingDelay)
    {
        this.rowsPerPage = requireNonNull(rowsPerPage, "rowsPerPage is null");
        this.pagesCount = requireNonNull(pagesCount, "pagesCount is null");
        this.fieldsLength = requireNonNull(fieldsLength, "fieldsLength is null");
        this.pageProcessingDelay = requireNonNull(pageProcessingDelay, "pageProcessingDelay is null");
    }

    @JsonProperty
    public int getPagesCount()
    {
        return pagesCount;
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
    public boolean isRemotelyAccessible()
    {
        return true;
    }

    @Override
    public List<HostAddress> getAddresses()
    {
        return ImmutableList.of();
    }

    @Override
    public Object getInfo()
    {
        return this;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(getPagesCount(), getRowsPerPage());
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
        BlackHoleSplit other = (BlackHoleSplit) obj;
        return Objects.equals(this.getPagesCount(), other.getPagesCount()) &&
                Objects.equals(this.getRowsPerPage(), other.getRowsPerPage());
    }
}
