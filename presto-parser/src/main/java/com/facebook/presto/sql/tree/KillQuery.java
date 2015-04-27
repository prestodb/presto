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
package com.facebook.presto.sql.tree;

import java.util.Objects;

import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkNotNull;

public class KillQuery
        extends Statement
{
    private final String queryID;

    public KillQuery(String queryID)
    {
        this.queryID = checkNotNull(queryID, "queryID is null");
    }

    public String getQueryID()
    {
        return queryID;
    }

    @Override
    public <R, C> R accept(AstVisitor<R, C> visitor, C context)
    {
        return visitor.visitKillQuery(this, context);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(queryID);
    }

    @Override
    public boolean equals(Object obj)
    {
        if (this == obj) {
            return true;
        }
        if ((obj == null) || (getClass() != obj.getClass())) {
            return false;
        }
        KillQuery o = (KillQuery) obj;
        return Objects.equals(queryID, o.queryID);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("queryID", queryID)
                .toString();
    }
}
