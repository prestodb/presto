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
package com.facebook.presto.accumulo.serializers;

import org.apache.accumulo.core.client.lexicoder.Lexicoder;

/**
 * Accumulo lexicoder for Booleans
 */
public class BooleanLexicoder
        implements Lexicoder<Boolean>
{
    public static final byte[] TRUE = new byte[] {1};
    public static final byte[] FALSE = new byte[] {0};

    @Override
    public byte[] encode(Boolean v)
    {
        return v ? TRUE : FALSE;
    }

    @Override
    public Boolean decode(byte[] b)
    {
        return b[0] != 0;
    }
}
