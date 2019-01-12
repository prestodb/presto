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
package io.prestosql.orc.metadata;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;

public class StripeInformation
{
    private final int numberOfRows;
    private final long offset;
    private final long indexLength;
    private final long dataLength;
    private final long footerLength;

    public StripeInformation(int numberOfRows, long offset, long indexLength, long dataLength, long footerLength)
    {
        // dataLength can be zero when the stripe only contains empty flat maps.
        checkArgument(numberOfRows > 0, "Stripe must have at least one row");
        checkArgument(footerLength > 0, "Stripe must have a footer section");
        this.numberOfRows = numberOfRows;
        this.offset = offset;
        this.indexLength = indexLength;
        this.dataLength = dataLength;
        this.footerLength = footerLength;
    }

    public int getNumberOfRows()
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
}
