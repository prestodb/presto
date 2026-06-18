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
package com.facebook.presto.plugin.clickhouse;

public class TimestampUtil
{
    private TimestampUtil() {}

    public static int getMillisecondsFromTimestampString(String timestampString)
    {
        int dotIndex = timestampString.indexOf('.');
        if (dotIndex == -1) {
            return 0;
        }

        String fraction = timestampString.substring(dotIndex + 1);
        int nonNormalized = Integer.parseInt(fraction);
        if (nonNormalized == 0 || fraction.length() == 3) {
            return nonNormalized;
        }
        // this will make sure it's always 3 digits. e.g., 7 -> 700; 71 -> 710; 7591 -> 759
        return (int) Math.round(nonNormalized * Math.pow(10, -(Math.floor(Math.log10(nonNormalized)) - 2)));
    }
}
