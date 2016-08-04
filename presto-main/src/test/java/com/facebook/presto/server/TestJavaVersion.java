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
package com.facebook.presto.server;

import org.testng.annotations.Test;

import java.util.OptionalInt;

import static org.testng.Assert.assertEquals;

public class TestJavaVersion
{
    @Test
    public void testParseLegacy()
            throws Exception
    {
        assertEquals(JavaVersion.parse("1.8"), new JavaVersion(8, 0));
        assertEquals(JavaVersion.parse("1.8.0"), new JavaVersion(8, 0));
        assertEquals(JavaVersion.parse("1.8.0_5"), new JavaVersion(8, 0, OptionalInt.of(5)));
        assertEquals(JavaVersion.parse("1.8.0_20"), new JavaVersion(8, 0, OptionalInt.of(20)));
        assertEquals(JavaVersion.parse("1.8.1_25"), new JavaVersion(8, 1, OptionalInt.of(25)));
    }

    @Test
    public void testParseNew()
            throws Exception
    {
        assertEquals(JavaVersion.parse("9-ea+19"), new JavaVersion(9, 0));
        assertEquals(JavaVersion.parse("9+100"), new JavaVersion(9, 0));
        assertEquals(JavaVersion.parse("9.0.1+20"), new JavaVersion(9, 0));
        assertEquals(JavaVersion.parse("9.1.1+20"), new JavaVersion(9, 1));
    }
}
