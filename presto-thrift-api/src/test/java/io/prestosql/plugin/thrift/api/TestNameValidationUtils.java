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
package io.prestosql.plugin.thrift.api;

import org.testng.annotations.Test;

import static io.prestosql.plugin.thrift.api.NameValidationUtils.checkValidName;
import static org.testng.Assert.assertThrows;

public class TestNameValidationUtils
{
    @Test
    public void testCheckValidColumnName()
    {
        checkValidName("abc01_def2");
        assertThrows(() -> checkValidName(null));
        assertThrows(() -> checkValidName(""));
        assertThrows(() -> checkValidName("Abc"));
        assertThrows(() -> checkValidName("0abc"));
        assertThrows(() -> checkValidName("_abc"));
        assertThrows(() -> checkValidName("aBc"));
        assertThrows(() -> checkValidName("ab-c"));
    }
}
