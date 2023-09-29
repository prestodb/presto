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
package com.facebook.presto.spi;

import static com.facebook.presto.spi.StandardErrorCode.NOT_FOUND;
import static java.lang.String.format;

final class SchemaUtil
{
    private SchemaUtil()
    {
    }

    static String checkNotEmpty(String value, String name)
    {
        if (value == null) {
            throw new NullPointerException(name + " is null");
        }
        if (value.isEmpty()) {
            throw new PrestoException(NOT_FOUND, format("%s is empty", name));
        }
        return value;
    }
}
