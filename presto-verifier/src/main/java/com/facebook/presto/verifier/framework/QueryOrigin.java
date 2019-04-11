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

import static com.facebook.presto.verifier.framework.QueryOrigin.QueryStage.CHECKSUM;
import static com.facebook.presto.verifier.framework.QueryOrigin.QueryStage.DESCRIBE;
import static com.facebook.presto.verifier.framework.QueryOrigin.QueryStage.MAIN;
import static com.facebook.presto.verifier.framework.QueryOrigin.QueryStage.REWRITE;
import static com.facebook.presto.verifier.framework.QueryOrigin.QueryStage.SETUP;
import static com.facebook.presto.verifier.framework.QueryOrigin.QueryStage.TEARDOWN;
import static com.facebook.presto.verifier.framework.QueryOrigin.TargetCluster.CONTROL;
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
        SETUP,
        MAIN,
        TEARDOWN,
        REWRITE,
        DESCRIBE,
        CHECKSUM,
    }

    private final TargetCluster cluster;
    private final QueryStage stage;

    private QueryOrigin(TargetCluster cluster, QueryStage stage)
    {
        this.cluster = requireNonNull(cluster, "cluster is null");
        this.stage = requireNonNull(stage, "stage is null");
    }

    public static QueryOrigin forSetup(TargetCluster group)
    {
        return new QueryOrigin(group, SETUP);
    }

    public static QueryOrigin forMain(TargetCluster group)
    {
        return new QueryOrigin(group, MAIN);
    }

    public static QueryOrigin forTeardown(TargetCluster group)
    {
        return new QueryOrigin(group, TEARDOWN);
    }

    public static QueryOrigin forRewrite()
    {
        return new QueryOrigin(CONTROL, REWRITE);
    }

    public static QueryOrigin forDescribe()
    {
        return new QueryOrigin(CONTROL, DESCRIBE);
    }

    public static QueryOrigin forChecksum()
    {
        return new QueryOrigin(CONTROL, CHECKSUM);
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
