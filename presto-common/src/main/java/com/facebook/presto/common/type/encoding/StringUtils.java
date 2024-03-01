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

package com.facebook.presto.common.type.encoding;

import java.nio.charset.Charset;

public final class StringUtils
{
    private StringUtils() {}

    public static final Charset UTF_8 = Charset.forName("UTF-8");

    static byte[] getBytesUtf8(final String string)
    {
        if (string == null) {
            return null;
        }
        return string.getBytes(StringUtils.UTF_8);
    }

    static String newStringUtf8(final byte[] bytes)
    {
        return bytes == null ? null : new String(bytes, StringUtils.UTF_8);
    }
}
