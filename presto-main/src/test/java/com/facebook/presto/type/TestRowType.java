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

import com.facebook.presto.spi.type.Type;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Optional;

import static com.facebook.presto.spi.type.BooleanType.BOOLEAN;
import static com.facebook.presto.spi.type.DoubleType.DOUBLE;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static java.util.Arrays.asList;
import static org.testng.Assert.assertEquals;

public class TestRowType
{
    @Test
    public void testRowDisplayName()
    {
        List<Type> types = asList(BOOLEAN, DOUBLE, new ArrayType(VARCHAR), new MapType(BOOLEAN, DOUBLE));
        Optional<List<String>> names = Optional.of(asList("bool_col", "double_col", "array_col", "map_col"));
        RowType row = new RowType(types, names);
        assertEquals(
                row.getDisplayName(),
                "row(bool_col boolean, double_col double, array_col array(varchar), map_col map(boolean, double))");
    }

    @Test
    public void testRowDisplayNoColumnNames()
    {
        List<Type> types = asList(BOOLEAN, DOUBLE, new ArrayType(VARCHAR), new MapType(BOOLEAN, DOUBLE));
        RowType row = new RowType(types, Optional.empty());
        assertEquals(
                row.getDisplayName(),
                "row(boolean, double, array(varchar), map(boolean, double))");
    }
}
