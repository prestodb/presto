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
package com.facebook.presto.hive.coercions;

import com.facebook.presto.spi.block.BlockBuilder;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.RealType.REAL;
import static java.lang.Float.intBitsToFloat;

public class FloatToDoubleCoercer
        extends AbstractCoercer
{
    protected FloatToDoubleCoercer()
    {
        super(REAL, DOUBLE);
    }

    @Override
    protected void appendCoercedLong(BlockBuilder blockBuilder, long aLong)
    {
        DOUBLE.writeDouble(blockBuilder, intBitsToFloat((int) aLong));
    }
}
