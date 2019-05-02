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
package com.facebook.presto.verifier.framework;

import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class QueryOrigin
{
    public enum QueryGroup
    {
        CONTROL,
        TEST,
    }

    public enum QueryStage
    {
        REWRITE,
        SETUP,
        MAIN,
        TEARDOWN,
        DESCRIBE,
        CHECKSUM,
    }

    private final QueryGroup group;
    private final QueryStage stage;

    public QueryOrigin(QueryGroup group, QueryStage stage)
    {
        this.group = requireNonNull(group, "group is null");
        this.stage = requireNonNull(stage, "stage is null");
    }

    public QueryGroup getGroup()
    {
        return group;
    }

    public QueryStage getStage()
    {
        return stage;
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
        QueryOrigin o = (QueryOrigin) obj;
        return Objects.equals(group, o.group) &&
                Objects.equals(stage, o.stage);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(group, stage);
    }
}
