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

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import org.testng.annotations.Test;

import static com.facebook.presto.spi.type.DoubleType.DOUBLE;

public class TestLegacyMapSubscript
        extends AbstractTestFunctions
{
    private TestLegacyMapSubscript()
    {
        super(new FeaturesConfig().setLegacyMapSubscript(true));
    }

    @Test
    public void testSubscript()
            throws Exception
    {
        assertFunction("MAP(ARRAY [1], ARRAY [1.5])[2]", DOUBLE, null);
    }
}
