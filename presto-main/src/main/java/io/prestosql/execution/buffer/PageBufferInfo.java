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
package io.prestosql.execution.buffer;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;

public class PageBufferInfo
{
    private final int partition;
    private final long bufferedPages;
    private final long bufferedBytes;
    private final long rowsAdded;
    private final long pagesAdded;

    @JsonCreator
    public PageBufferInfo(
            @JsonProperty("partition") int partition,
            @JsonProperty("bufferedPages") long bufferedPages,
            @JsonProperty("bufferedBytes") long bufferedBytes,
            @JsonProperty("rowsAdded") long rowsAdded,
            @JsonProperty("pagesAdded") long pagesAdded)
    {
        this.partition = partition;
        this.bufferedPages = bufferedPages;
        this.bufferedBytes = bufferedBytes;
        this.rowsAdded = rowsAdded;
        this.pagesAdded = pagesAdded;
    }

    @JsonProperty
    public int getPartition()
    {
        return partition;
    }

    @JsonProperty
    public long getBufferedPages()
    {
        return bufferedPages;
    }

    @JsonProperty
    public long getBufferedBytes()
    {
        return bufferedBytes;
    }

    @JsonProperty
    public long getRowsAdded()
    {
        return rowsAdded;
    }

    @JsonProperty
    public long getPagesAdded()
    {
        return pagesAdded;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (o == null || getClass() != o.getClass()) {
            return false;
        }
        PageBufferInfo that = (PageBufferInfo) o;
        return Objects.equals(partition, that.partition) &&
                Objects.equals(bufferedPages, that.bufferedPages) &&
                Objects.equals(bufferedBytes, that.bufferedBytes) &&
                Objects.equals(rowsAdded, that.rowsAdded) &&
                Objects.equals(pagesAdded, that.pagesAdded);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(partition, bufferedPages, bufferedBytes, rowsAdded, pagesAdded);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("partition", partition)
                .add("bufferedPages", bufferedPages)
                .add("bufferedBytes", bufferedBytes)
                .add("rowsAdded", rowsAdded)
                .add("pagesAdded", pagesAdded)
                .toString();
    }

    public static PageBufferInfo empty()
    {
        return new PageBufferInfo(0, 0, 0, 0, 0);
    }
}
