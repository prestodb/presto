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
package com.facebook.presto.pinot;

import com.facebook.presto.testing.assertions.Assert;
import org.testng.annotations.BeforeTest;
import org.testng.annotations.Test;

import java.util.ArrayList;
import java.util.List;

import static com.facebook.presto.pinot.PinotQueryGenerator.getPinotQuery;
import static com.facebook.presto.spi.type.BigintType.BIGINT;
import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.IntegerType.INTEGER;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;

public class PinotQueryGeneratorTest
{
    final List<PinotColumnHandle> columnHandles = new ArrayList<>();

    @BeforeTest
    void init()
    {
        columnHandles.add(new PinotColumnHandle("pinot", "varchar", VARCHAR, 0));
        columnHandles.add(new PinotColumnHandle("pinot", "int", INTEGER, 1));
        columnHandles.add(new PinotColumnHandle("pinot", "secondsSinceEpoch", BIGINT, 2));
        columnHandles.add(new PinotColumnHandle("pinot", "boolean", BOOLEAN, 3));
        columnHandles.add(new PinotColumnHandle("pinot", "double", DOUBLE, 4));
    }

    @Test
    public void testGetPinotQuerySelectAll()
    {
        String expectedQuery = "SELECT varchar, int, secondsSinceEpoch, boolean, double FROM table  LIMIT 10";
        Assert.assertEquals(expectedQuery, getPinotQuery(new PinotConfig(), columnHandles, "", "", "table", 10));
    }

    @Test
    public void testGetPinotQueryWithPredicate()
    {
        String expectedQuery = "SELECT varchar, int, secondsSinceEpoch, boolean, double FROM table WHERE ((int > 3)) AND ((secondsSinceEpoch > 10000)) LIMIT 10";
        Assert.assertEquals(expectedQuery, getPinotQuery(new PinotConfig(), columnHandles, "(int > 3)", "(secondsSinceEpoch > 10000)", "table", 10));
    }
}
