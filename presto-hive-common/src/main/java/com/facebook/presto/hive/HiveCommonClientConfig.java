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
package com.facebook.presto.hive;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;

public class HiveCommonClientConfig
{
    private boolean rangeFiltersOnSubscriptsEnabled;
    public boolean isRangeFiltersOnSubscriptsEnabled()
    {
        return rangeFiltersOnSubscriptsEnabled;
    }

    @Config("hive.range-filters-on-subscripts-enabled")
    @ConfigDescription("Experimental: enable pushdown of range filters on subscripts (a[2] = 5) into ORC column readers")
    public HiveCommonClientConfig setRangeFiltersOnSubscriptsEnabled(boolean rangeFiltersOnSubscriptsEnabled)
    {
        this.rangeFiltersOnSubscriptsEnabled = rangeFiltersOnSubscriptsEnabled;
        return this;
    }
}
