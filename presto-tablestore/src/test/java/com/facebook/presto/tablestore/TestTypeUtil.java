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
package com.facebook.presto.tablestore;

import org.testng.annotations.Test;

import static com.facebook.presto.tablestore.TypeUtil.checkType;
import static org.testng.Assert.assertTrue;
import static org.testng.Assert.fail;

public class TestTypeUtil
{
    @Test
    public void testCheckType()
    {
        try {
            checkType(null, String.class);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("but it's null"));
        }

        try {
            checkType(1, String.class);
            fail();
        }
        catch (Exception e) {
            assertTrue(e.getMessage().contains("but it's the type[java.lang.Integer]."));
        }

        checkType(1, Object.class);
    }
}
