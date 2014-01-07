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
package com.facebook.presto.operator;

public enum SortOrder
{
    ASC_NULLS_FIRST(true, true),
    ASC_NULLS_LAST(true, false),
    DESC_NULLS_FIRST(false, true),
    DESC_NULLS_LAST(false, false);

    private final boolean ascending;
    private final boolean nullsFirst;

    private SortOrder(boolean ascending, boolean nullsFirst)
    {
        this.ascending = ascending;
        this.nullsFirst = nullsFirst;
    }

    public boolean isAscending()
    {
        return ascending;
    }

    public boolean isNullsFirst()
    {
        return nullsFirst;
    }
}
