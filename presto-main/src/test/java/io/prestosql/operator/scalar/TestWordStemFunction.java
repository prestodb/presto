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

import org.testng.annotations.Test;

import static io.prestosql.spi.type.VarcharType.createVarcharType;

public class TestWordStemFunction
        extends AbstractTestFunctions
{
    @Test
    public void testWordStep()
    {
        assertFunction("word_stem('')", createVarcharType(0), "");
        assertFunction("word_stem('x')", createVarcharType(1), "x");
        assertFunction("word_stem('abc')", createVarcharType(3), "abc");
        assertFunction("word_stem('generally')", createVarcharType(9), "general");
        assertFunction("word_stem('useful')", createVarcharType(6), "use");
        assertFunction("word_stem('runs')", createVarcharType(4), "run");
        assertFunction("word_stem('run')", createVarcharType(3), "run");
        assertFunction("word_stem('authorized', 'en')", createVarcharType(10), "author");
        assertFunction("word_stem('accessories', 'en')", createVarcharType(11), "accessori");
        assertFunction("word_stem('intensifying', 'en')", createVarcharType(12), "intensifi");
        assertFunction("word_stem('resentment')", createVarcharType(10), "resent");
        assertFunction("word_stem('faithfulness')", createVarcharType(12), "faith");
        assertFunction("word_stem('continuerait', 'fr')", createVarcharType(12), "continu");
        assertFunction("word_stem('torpedearon', 'es')", createVarcharType(11), "torped");
        assertFunction("word_stem('quilomtricos', 'pt')", createVarcharType(12), "quilomtr");
        assertFunction("word_stem('pronunziare', 'it')", createVarcharType(11), "pronunz");
        assertFunction("word_stem('auferstnde', 'de')", createVarcharType(10), "auferstnd");

        assertInvalidFunction("word_stem('test', 'xx')", "Unknown stemmer language: xx");
    }
}
