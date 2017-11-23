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
package com.facebook.presto.orc.metadata;

import io.airlift.slice.Slice;

import java.util.Optional;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StripeInformation
{
    private final long rowOffsetInFile;
    private final int numberOfRows;
    private final long offset;
    private final Optional<Slice> indexData;
    private final int indexLength;
    private final long dataLength;
    private final Optional<Slice> footerData;
    private final int footerLength;

    public StripeInformation(long rowOffsetInFile, int numberOfRows, long offset, int indexLength, long dataLength, int footerLength)
    {
        this(rowOffsetInFile, numberOfRows, offset, Optional.empty(), indexLength, dataLength, Optional.empty(), footerLength);
    }

    public StripeInformation(long rowOffsetInFile, int numberOfRows, long offset, Optional<Slice> indexData, int indexLength, long dataLength, Optional<Slice> footerData, int footerLength)
    {
        checkArgument(rowOffsetInFile >= 0, "rowOffsetInFile is negative");
        checkArgument(numberOfRows > 0, "Stripe must have at least one row");

        requireNonNull(indexData, "indexData is null");
        checkArgument(!indexData.isPresent() || indexData.get().length() == indexLength, "indexData length must be the same as indexLength");

        checkArgument(dataLength > 0, "Stripe must have a data section");

        requireNonNull(footerData, "footerData is null");
        checkArgument(footerLength > 0, "Stripe must have a footer section");
        checkArgument(!footerData.isPresent() || footerData.get().length() == footerLength, "footerData length must be the same as footerLength");

        this.rowOffsetInFile = rowOffsetInFile;
        this.numberOfRows = numberOfRows;
        this.offset = offset;
        this.indexData = indexData;
        this.indexLength = indexLength;
        this.dataLength = dataLength;
        this.footerData = footerData;
        this.footerLength = footerLength;
    }

    public long getRowOffsetInFile()
    {
        return rowOffsetInFile;
    }

    public int getNumberOfRows()
    {
        return numberOfRows;
    }

    public long getOffset()
    {
        return offset;
    }

    public Optional<Slice> getIndexData()
    {
        return indexData;
    }

    public int getIndexLength()
    {
        return indexLength;
    }

    public long getDataLength()
    {
        return dataLength;
    }

    public Optional<Slice> getFooterData()
    {
        return footerData;
    }

    public int getFooterLength()
    {
        return footerLength;
    }

    public long getTotalLength()
    {
        return indexLength + dataLength + footerLength;
    }

    public StripeInformation withData(Optional<Slice> indexData, Optional<Slice> footerData)
    {
        return new StripeInformation(rowOffsetInFile, numberOfRows, offset, indexData, indexLength, dataLength, footerData, footerLength);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("rowOffsetInFile", rowOffsetInFile)
                .add("numberOfRows", numberOfRows)
                .add("offset", offset)
                .add("indexLength", indexLength)
                .add("dataLength", dataLength)
                .add("footerLength", footerLength)
                .toString();
    }
}
