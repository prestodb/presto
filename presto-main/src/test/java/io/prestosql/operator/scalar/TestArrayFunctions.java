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
package io.prestosql.operator.scalar;

import com.google.common.base.Joiner;
import io.prestosql.spi.type.ArrayType;
import org.testng.annotations.Test;

import static io.prestosql.spi.type.DoubleType.DOUBLE;
import static io.prestosql.spi.type.IntegerType.INTEGER;
import static java.util.Collections.nCopies;

public class TestArrayFunctions
        extends AbstractTestFunctions
{
    @Test
    public void testArrayConstructor()
    {
        tryEvaluateWithAll("array[" + Joiner.on(", ").join(nCopies(254, "rand()")) + "]", new ArrayType(DOUBLE));
        assertNotSupported(
                "array[" + Joiner.on(", ").join(nCopies(255, "rand()")) + "]",
                "Too many arguments for array constructor");
    }

    @Test
    public void testArrayConcat()
    {
        assertFunction("CONCAT(" + Joiner.on(", ").join(nCopies(253, "array[1]")) + ")", new ArrayType(INTEGER), nCopies(253, 1));
        assertNotSupported(
                "CONCAT(" + Joiner.on(", ").join(nCopies(254, "array[1]")) + ")",
                "Too many arguments for vararg function");
    }
}
