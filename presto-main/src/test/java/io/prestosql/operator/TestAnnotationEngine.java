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
package io.prestosql.operator;

import io.prestosql.operator.aggregation.AggregationImplementation;
import io.prestosql.operator.scalar.ParametricScalar;

import static org.testng.Assert.assertEquals;

abstract class TestAnnotationEngine
{
    void assertImplementationCount(ParametricScalar scalar, int exact, int specialized, int generic)
    {
        assertImplementationCount(scalar.getImplementations(), exact, specialized, generic);
    }

    void assertImplementationCount(ParametricImplementationsGroup<?> implementations, int exact, int specialized, int generic)
    {
        assertEquals(implementations.getExactImplementations().size(), exact);
        assertEquals(implementations.getSpecializedImplementations().size(), specialized);
        assertEquals(implementations.getGenericImplementations().size(), generic);
    }

    void assertDependencyCount(AggregationImplementation implementation, int input, int combine, int output)
    {
        assertEquals(implementation.getInputDependencies().size(), input);
        assertEquals(implementation.getCombineDependencies().size(), combine);
        assertEquals(implementation.getOutputDependencies().size(), output);
    }
}
