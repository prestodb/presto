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

package com.facebook.presto.nativeworker;

import org.testng.annotations.Test;

public class TestHiveBitwiseFunctionQueries
        extends AbstractTestHiveQueries
{
    public TestHiveBitwiseFunctionQueries()
    {
        super(false);
    }

    @Test
    public void testBitCount()
    {
        this.assertQuery("SELECT nationkey, bit_count(nationkey, 10) FROM nation ORDER BY 1");
        this.assertQueryFails("SELECT nationkey, bit_count(nationkey, 2) FROM nation ORDER BY 1",
                ".*Number must be representable with the bits specified.*");
        this.assertQueryFails("SELECT nationkey, bit_count(nationkey, 1) FROM nation ORDER BY 1",
                ".*Bits specified in bit_count must be between 2 and 64, got 1.*");
        this.assertQueryFails("SELECT nationkey, bit_count(nationkey, 65) FROM nation ORDER BY 1",
                ".*Bits specified in bit_count must be between 2 and 64, got 65.*");
    }
}
