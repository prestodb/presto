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
package com.facebook.presto.plugin.turbonium.encodings;

import com.facebook.presto.plugin.turbonium.stats.Stats;
import com.facebook.presto.plugin.turbonium.storage.BooleanSegments.AllValues;
import com.facebook.presto.plugin.turbonium.storage.BooleanSegments.Rle;
import com.facebook.presto.plugin.turbonium.storage.BooleanSegments.RleWithNulls;
import com.facebook.presto.plugin.turbonium.storage.NullSegment;
import com.facebook.presto.plugin.turbonium.storage.Segment;
import com.facebook.presto.spi.type.Type;

import java.util.BitSet;

public class BooleanEncoder
    extends AbstractEncoder<Boolean>
{
    private final boolean[] values;
    private final BitSet isNull;
    public BooleanEncoder(boolean disableEncoding, Stats<Boolean> stats, Type type, BitSet isNull, boolean[] values)
    {
        super(type, stats, disableEncoding);
        this.values = values;
        this.isNull = isNull;
    }

    @Override
    protected boolean doDictionary()
    {
        return false;
    }

    @Override
    protected boolean doDelta()
    {
        return false;
    }

    @Override
    public Segment encode()
    {
        switch (encoding) {
            case NONE:
                return new AllValues(type, isNull, stats, values);
            case NULL:
                return new NullSegment(type, stats.size());
            case RLE:
                return new Rle(type, stats);
            case RLE_NULL:
                return new RleWithNulls(type, isNull, stats);
            default:
                throw new IllegalStateException("undefined encoding");
        }
    }
}
