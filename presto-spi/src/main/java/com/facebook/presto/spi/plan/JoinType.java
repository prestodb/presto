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
package com.facebook.presto.spi.plan;

import java.util.List;

public enum JoinType
{
    INNER("InnerJoin"),
    LEFT("LeftJoin"),
    RIGHT("RightJoin"),
    FULL("FullJoin"),
    SOURCE_OUTER("SourceOuter");

    private final String joinLabel;

    JoinType(String joinLabel)
    {
        this.joinLabel = joinLabel;
    }

    public String getJoinLabel()
    {
        return joinLabel;
    }

    public boolean mustPartition()
    {
        // With REPLICATED, the unmatched rows from right-side would be duplicated.
        return this == RIGHT || this == FULL;
    }

    public boolean mustReplicate(List<EquiJoinClause> criteria)
    {
        // There is nothing to partition on
        return criteria.isEmpty() && (this == INNER || this == LEFT);
    }
}
