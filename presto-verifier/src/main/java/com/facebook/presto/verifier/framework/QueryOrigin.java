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
    public enum TargetCluster
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

    private final TargetCluster cluster;
    private final QueryStage stage;

    public QueryOrigin(TargetCluster cluster, QueryStage stage)
    {
        this.cluster = requireNonNull(cluster, "cluster is null");
        this.stage = requireNonNull(stage, "stage is null");
    }

    public TargetCluster getCluster()
    {
        return cluster;
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
        return Objects.equals(cluster, o.cluster) &&
                Objects.equals(stage, o.stage);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(cluster, stage);
    }
}
