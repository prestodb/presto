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
package com.facebook.presto.pinot;

import com.google.common.base.CharMatcher;
import com.google.common.base.Splitter;
import com.google.common.base.Strings;
import com.google.common.collect.ImmutableMap;

import java.util.Map;
import java.util.stream.Collectors;

import static com.google.common.base.Strings.isNullOrEmpty;

public class PinotQueryOptionsUtils
{
    private static final Splitter.MapSplitter MAP_SPLITTER = Splitter.on(",").trimResults().omitEmptyStrings().withKeyValueSeparator(":");

    private PinotQueryOptionsUtils()
    {
    }

    public static boolean isNullHandlingEnabled(String queryOptions)
    {
        if (isNullOrEmpty(queryOptions)) {
            return false;
        }
        Map<String, String> queryOptionsMap = ImmutableMap.copyOf(MAP_SPLITTER.split(queryOptions));
        return Boolean.parseBoolean(queryOptionsMap.get(PinotQueryOptionKey.ENABLE_NULL_HANDLING));
    }

    public static String getQueryOptionsAsString(String queryOptions)
    {
        if (isNullOrEmpty(queryOptions)) {
            return "";
        }

        Map<String, String> queryOptionsMap = ImmutableMap.copyOf(MAP_SPLITTER.split(queryOptions));
        if (queryOptionsMap.isEmpty()) {
            return "";
        }
        return queryOptionsMap.entrySet().stream()
                .filter(kv -> !Strings.isNullOrEmpty(kv.getKey()) && !Strings.isNullOrEmpty(kv.getValue()))
                .map(kv -> CharMatcher.anyOf("\"`'").trimFrom(kv.getKey()) + "=" + kv.getValue())
                .collect(Collectors.joining(",", " option(", ") "));
    }
}
