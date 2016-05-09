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

import com.facebook.presto.spi.type.VarcharType;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static org.testng.Assert.assertEquals;

public class TestMapType
{
    @Test
    public void testMapDisplayName()
    {
        MapType mapType = new MapType(BIGINT, VarcharType.createVarcharType(42));
        assertEquals(mapType.getDisplayName(), "map(bigint, varchar(42))");

        mapType = new MapType(BIGINT, VARCHAR);
        assertEquals(mapType.getDisplayName(), "map(bigint, varchar)");
    }
}
