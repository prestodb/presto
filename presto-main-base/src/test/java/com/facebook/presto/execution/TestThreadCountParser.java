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
package com.facebook.presto.execution;

import org.testng.annotations.Test;

import static com.facebook.presto.execution.ThreadCountParser.parse;
import static org.assertj.core.api.Assertions.assertThatThrownBy;
import static org.testng.Assert.assertEquals;

public class TestThreadCountParser
{
    @Test
    public void testParse()
    {
        assertEquals(parse("1"), 1);
        assertEquals(parse("100"), 100);
        assertEquals(parse("1", 1), 1);
        assertEquals(parse("1", 2), 1);
        assertEquals(parse("100", 4), 100);
        assertEquals(parse("1C", 4), 4);
        assertEquals(parse("2C", 8), 16);
        assertEquals(parse("1.5C", 6), 9);
        assertEquals(parse("0.1C", 1), 1);
        assertThatThrownBy(() -> parse("0"))
                .hasMessageContaining("Thread count must be positive");
        assertThatThrownBy(() -> parse("-1"))
                .hasMessageContaining("Thread count must be positive");
        assertThatThrownBy(() -> parse("0C"))
                .hasMessageContaining("Thread multiplier must be positive");
        assertThatThrownBy(() -> parse("-1C"))
                .hasMessageContaining("Thread multiplier must be positive");
        assertThatThrownBy(() -> parse("abc"))
                .isInstanceOf(NumberFormatException.class);
        assertThatThrownBy(() -> parse("abcC"))
                .isInstanceOf(NumberFormatException.class);
    }
}
