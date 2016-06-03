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
package com.facebook.presto.sql;

import com.facebook.presto.sql.parser.SqlParser;
import com.facebook.presto.sql.tree.Statement;

import org.testng.annotations.Test;

import java.io.ByteArrayOutputStream;
import java.io.PrintStream;
import java.util.IdentityHashMap;

import static org.testng.Assert.assertEquals;

public class TestTreePrinter
{
    @Test
    public void testGroupByClause()
            throws Exception
    {
        Statement ast = new SqlParser().createStatement("SELECT a, b, c FROM t1 GROUP BY CUBE(a, b), ROLLUP(a, c), GROUPING SETS((a, b, c))");

        assertEquals(printToString(ast),
                "Query \n" +
                "   QueryBody\n" +
                "   QuerySpecification \n" +
                "      Select\n" +
                "            QualifiedName[a]\n" +
                "            QualifiedName[b]\n" +
                "            QualifiedName[c]\n" +
                "      From\n" +
                "         Table[t1]\n" +
                "      GroupBy\n" +
                "         Cube\n" +
                "            QualifiedName[a]\n" +
                "            QualifiedName[b]\n" +
                "         Rollup\n" +
                "            QualifiedName[a]\n" +
                "            QualifiedName[c]\n" +
                "         GroupingSets\n" +
                "            GroupingSet[\n" +
                "               QualifiedName[a]\n" +
                "               QualifiedName[b]\n" +
                "               QualifiedName[c]\n" +
                "            ]" +
                "\n");
    }

    static String printToString(Statement ast)
            throws Exception
    {
        ByteArrayOutputStream os = new ByteArrayOutputStream();
        PrintStream ps = new PrintStream(os);
        new TreePrinter(new IdentityHashMap<>(), ps).print(ast);
        return os.toString("UTF8");
    }
}
