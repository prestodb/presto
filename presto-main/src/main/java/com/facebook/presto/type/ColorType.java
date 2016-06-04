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

import com.facebook.presto.operator.scalar.ColorFunctions;
import com.facebook.presto.spi.ConnectorSession;
import com.facebook.presto.spi.block.Block;
import com.facebook.presto.spi.type.AbstractIntType;

import static com.facebook.presto.type.TypeUtils.parameterizedTypeName;

public class ColorType
        extends AbstractIntType
{
    public static final ColorType COLOR = new ColorType();
    public static final String NAME = "color";

    private ColorType()
    {
        super(parameterizedTypeName(NAME));
    }

    @Override
    public boolean isOrderable()
    {
        return false;
    }

    @Override
    public int compareTo(Block leftBlock, int leftPosition, Block rightBlock, int rightPosition)
    {
        throw new UnsupportedOperationException();
    }

    @Override
    public Object getObjectValue(ConnectorSession session, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        int color = block.getInt(position, 0);
        if (color < 0) {
            return ColorFunctions.SystemColor.valueOf(-(color + 1)).getName();
        }

        return String.format("#%02x%02x%02x",
                (color >> 16) & 0xFF,
                (color >> 8) & 0xFF,
                color & 0xFF);
    }

    @Override
    @SuppressWarnings("EqualsWhichDoesntCheckParameterClass")
    public boolean equals(Object other)
    {
        return other == COLOR;
    }

    @Override
    public int hashCode()
    {
        return getClass().hashCode();
    }
}
