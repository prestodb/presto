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

import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.OptionalLong;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.util.Objects.requireNonNull;

public class StripeInformation
{
    private final long numberOfRows;
    private final long offset;
    private final long indexLength;
    private final long dataLength;
    private final long footerLength;
    private final OptionalLong rawDataSize;

    // Arbitrary binary representing key metadata. It could be identifier
    // of key in KMS, encrypted DEK or other form of user defined key metadata.
    // only set for run start, and reuse until next run
    private final List<byte[]> keyMetadata;

    public StripeInformation(long numberOfRows, long offset, long indexLength, long dataLength, long footerLength, OptionalLong rawDataSize, List<byte[]> keyMetadata)
    {
        // dataLength can be zero when the stripe only contains empty flat maps.
        checkArgument(numberOfRows > 0, "Stripe must have at least one row");
        checkArgument(footerLength > 0, "Stripe must have a footer section");
        requireNonNull(keyMetadata, "keyMetadata is null");
        this.numberOfRows = numberOfRows;
        this.offset = offset;
        this.indexLength = indexLength;
        this.dataLength = dataLength;
        this.footerLength = footerLength;
        this.rawDataSize = requireNonNull(rawDataSize, "rawDataSize is null");
        this.keyMetadata = ImmutableList.copyOf(requireNonNull(keyMetadata, "keyMetadata is null"));
    }

    public long getNumberOfRows()
    {
        return numberOfRows;
    }

    public long getOffset()
    {
        return offset;
    }

    public long getIndexLength()
    {
        return indexLength;
    }

    public long getDataLength()
    {
        return dataLength;
    }

    public long getFooterLength()
    {
        return footerLength;
    }

    public long getTotalLength()
    {
        return indexLength + dataLength + footerLength;
    }

    public OptionalLong getRawDataSize()
    {
        return rawDataSize;
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("numberOfRows", numberOfRows)
                .add("offset", offset)
                .add("indexLength", indexLength)
                .add("dataLength", dataLength)
                .add("footerLength", footerLength)
                .toString();
    }

    public List<byte[]> getKeyMetadata()
    {
        return keyMetadata;
    }
}
