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
package com.teradata.presto.functions;

import org.testng.annotations.Test;

import java.nio.ByteBuffer;
import java.nio.CharBuffer;
import java.nio.charset.CharacterCodingException;
import java.nio.charset.StandardCharsets;

import static org.fest.assertions.Assertions.assertThat;

public class TestHexEncoder
{
    @Test
    public void testEncoder() throws CharacterCodingException
    {
        CharBuffer inputChars = CharBuffer.wrap(new char[]{'1', '2', '3', 'ಠ'});
        ByteBuffer hexBuffer = new HexEncoder().encode(inputChars);
        assertThat(StandardCharsets.US_ASCII.decode(hexBuffer).toString()).isEqualTo("0031003200330CA0");
    }
}
