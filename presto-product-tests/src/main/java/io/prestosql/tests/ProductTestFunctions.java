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
package io.prestosql.tests;

import io.prestodb.tempto.ProductTest;
import org.testng.annotations.Test;

import static io.prestodb.tempto.assertions.QueryAssert.Row.row;
import static io.prestodb.tempto.assertions.QueryAssert.assertThat;
import static io.prestosql.tests.TestGroups.FUNCTIONS;
import static io.prestosql.tests.utils.QueryExecutors.onPresto;

public class ProductTestFunctions
        extends ProductTest
{
    @Test(groups = FUNCTIONS)
    public void testSubstring()
    {
        assertThat(onPresto().executeQuery("SELECT SUBSTRING('ala ma kota' from 2 for 4)")).contains(row("la m"));
        assertThat(onPresto().executeQuery("SELECT SUBSTR('ala ma kota', 2, 4)")).contains(row("la m"));
    }

    @Test(groups = FUNCTIONS)
    public void testPosition()
    {
        assertThat(onPresto().executeQuery("SELECT POSITION('ma' IN 'ala ma kota')")).contains(row(5));
    }
}
