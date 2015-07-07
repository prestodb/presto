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

package com.facebook.presto.spi.type;

import org.testng.Assert;
import org.testng.annotations.Test;

public class TestSqlDecimal
{
    @Test
    public void testToString()
    {
        Assert.assertEquals(new SqlDecimal(0, 1, 1).toString(), "0.0");
        Assert.assertEquals(new SqlDecimal(0, 5, 2).toString(), "0.00");
        Assert.assertEquals(new SqlDecimal(0, 5, 5).toString(), "0.00000");
        Assert.assertEquals(new SqlDecimal(1, 1, 1).toString(), "0.1");
        Assert.assertEquals(new SqlDecimal(1, 5, 0).toString(), "1");
        Assert.assertEquals(new SqlDecimal(1, 5, 3).toString(), "0.001");
        Assert.assertEquals(new SqlDecimal(1000, 5, 3).toString(), "1.000");
    }
}
