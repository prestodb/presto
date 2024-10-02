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

package com.facebook.presto.operator.scalar;

import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.metadata.SqlScalarFunction;
import com.facebook.presto.operator.scalar.annotations.ScalarFromAnnotationsParser;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.LiteralParameters;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.ScalarFunctionConstantStats;
import com.facebook.presto.spi.function.ScalarPropagateSourceStats;
import com.facebook.presto.spi.function.ScalarStatsHeader;
import com.facebook.presto.spi.function.Signature;
import com.facebook.presto.spi.function.SqlType;
import com.facebook.presto.sql.analyzer.FeaturesConfig;
import io.airlift.slice.Slice;
import org.testng.annotations.Test;

import java.util.List;
import java.util.Map;

import static org.testng.Assert.assertEquals;
import static org.testng.Assert.assertTrue;

public class TestStatsAnnotationScalarFunctions
        extends AbstractTestFunctions
{
    public TestStatsAnnotationScalarFunctions()
    {
    }

    protected TestStatsAnnotationScalarFunctions(FeaturesConfig config)
    {
        super(config);
    }

    @Description("Functions with stats annotation")
    public static class TestScalarFunction
    {
        @SqlType(StandardTypes.BOOLEAN)
        @LiteralParameters("x")
        @ScalarFunction
        @ScalarFunctionConstantStats(avgRowSize = 2)
        public static boolean fun1(@SqlType("varchar(x)") Slice slice)
        {
            return true;
        }

        @SqlType(StandardTypes.BOOLEAN)
        @LiteralParameters("x")
        @ScalarFunction
        @ScalarFunctionConstantStats(avgRowSize = 2, distinctValuesCount = 2.0)
        public static boolean fun2(
                @ScalarPropagateSourceStats(propagateAllStats = true) @SqlType(StandardTypes.BIGINT) Slice slice)
        {
            return true;
        }
    }

    @Test
    public void testAnnotations()
    {
        List<SqlScalarFunction> sqlScalarFunctions = ScalarFromAnnotationsParser.parseFunctionDefinitions(TestScalarFunction.class);
        assertEquals(sqlScalarFunctions.size(), 2);
        for (SqlScalarFunction function : sqlScalarFunctions) {
            assertTrue(function instanceof ParametricScalar);
            ParametricScalar parametricScalar = (ParametricScalar) function;
            Signature signature = parametricScalar.getSignature().canonicalization();
            Map<Signature, ScalarStatsHeader> scalarStatsHeaderMap = parametricScalar.getScalarHeader().getSignatureToScalarStatsHeadersMap();
            ScalarStatsHeader scalarStatsHeader = scalarStatsHeaderMap.get(signature);
            assertEquals(scalarStatsHeader.getAvgRowSize(), 2);
            if (function.getSignature().getName().toString().equals("fun2")) {
                assertEquals(scalarStatsHeader.getDistinctValuesCount(), 2);
                Map<Integer, ScalarPropagateSourceStats> argumentStatsActual = scalarStatsHeader.getArgumentStats();
                assertEquals(argumentStatsActual.size(), 1);
                assertTrue(argumentStatsActual.get(0).propagateAllStats());
            }
        }
    }
}
