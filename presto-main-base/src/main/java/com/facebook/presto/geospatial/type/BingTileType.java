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
package com.facebook.presto.geospatial.type;

import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.function.SqlFunctionProperties;
import com.facebook.presto.common.type.AbstractLongType;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.geospatial.BingTile;

public class BingTileType
        extends AbstractLongType
{
    public static final BingTileType BING_TILE = new BingTileType();
    public static final String NAME = "BingTile";

    public BingTileType()
    {
        super(new TypeSignature(NAME));
    }

    @Override
    public boolean isOrderable()
    {
        return false;
    }

    @Override
    public Object getObjectValue(SqlFunctionProperties properties, Block block, int position)
    {
        if (block.isNull(position)) {
            return null;
        }

        return BingTile.decode(block.getLong(position));
    }
}
