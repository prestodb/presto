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
package com.facebook.presto.util;

import io.airlift.slice.XxHash64;

import static java.lang.Long.toHexString;
import static java.nio.charset.StandardCharsets.UTF_8;
import static java.util.Objects.requireNonNull;

public class QueryInfoUtils
{
    private QueryInfoUtils() {}

    public static String computeQueryHash(String query)
    {
        requireNonNull(query, "query is null");

        if (query.isEmpty()) {
            return "";
        }

        byte[] queryBytes = query.getBytes(UTF_8);
        long queryHash = new XxHash64().update(queryBytes).hash();
        return toHexString(queryHash);
    }
}
