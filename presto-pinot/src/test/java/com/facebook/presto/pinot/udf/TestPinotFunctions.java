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
package com.facebook.presto.pinot.udf;

import com.facebook.presto.operator.scalar.AbstractTestFunctions;
import com.facebook.presto.pinot.PinotPlugin;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static com.facebook.presto.SessionTestUtils.TEST_SESSION;
import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.metadata.FunctionExtractor.extractFunctions;

public class TestPinotFunctions
        extends AbstractTestFunctions
{
    public TestPinotFunctions()
    {
        super(TEST_SESSION, new FeaturesConfig(), new FunctionsConfig(), false);
    }

    @BeforeClass
    public void setUp()
    {
        functionAssertions.addFunctions(extractFunctions(new PinotPlugin().getFunctions()));
    }

    @Test
    public void testPinotBinaryDecimalToDouble() throws Exception
    {
        assertFunction("pinot_binary_decimal_to_double(CAST('' AS VARBINARY), 16, 18, true)", DOUBLE, 0D);
        assertFunction("pinot_binary_decimal_to_double(null, 16, 18, false)", DOUBLE, null);
        // "0DE0B6B3A7640000" = BigDecmial.ONE
        assertFunction("pinot_binary_decimal_to_double(from_hex('0DE0B6B3A7640000'), 16, 18, true)", DOUBLE, 1.0D);
    }
}
