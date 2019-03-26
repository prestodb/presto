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
package com.facebook.presto.orc;

import com.facebook.presto.spi.PageSourceOptions;

public class Filter
        extends PageSourceOptions.FilterStats
{
    boolean nullAllowed;

    public Filter(boolean nullAllowed)
    {
        this.nullAllowed = nullAllowed;
    }

    // True if one may call the filter once per distinct value. This
    // is usually true but a counter example is a filter on the data
    // column of a map where different positions have a different
    // filter.
    public boolean isDeterministic()
    {
        return true;
    }

    public boolean testLong(long value)
    {
        return false;
    }

    public boolean testDouble(double value)
    {
        return false;
    }

    public boolean testFloat(float value)
    {
        return false;
    }

    public boolean testDecimal(long low, long high)
    {
        return false;
    }

    public boolean testBoolean(boolean value)
    {
        return false;
    }

    public boolean testBytes(byte[] buffer, int offset, int length)
    {
        return false;
    }

    public boolean testNull()
    {
        return nullAllowed;
    }

    // If there are no scores, return a number for making initial filter order. Less is better.
    int staticScore()
    {
        return 100;
    }

    // Used for deciding wheher to make a hash based in when all ranges are equalities.
    public boolean isEquality()
    {
        return false;
    }
}
