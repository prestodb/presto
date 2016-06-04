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
package com.facebook.presto.type;

import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.AbstractLongType;
import com.facebook.presto.spi.type.StandardTypes;

import static com.facebook.presto.spi.type.TypeSignature.parseTypeSignature;

public final class IntervalYearMonthType
        extends AbstractLongType
{
    public static final IntervalYearMonthType INTERVAL_YEAR_MONTH = new IntervalYearMonthType();

    private IntervalYearMonthType()
    {
        super(parseTypeSignature(StandardTypes.INTERVAL_YEAR_TO_MONTH));
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }
        return new SqlIntervalYearMonth(block.getLong(position, 0));
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == INTERVAL_YEAR_MONTH;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
