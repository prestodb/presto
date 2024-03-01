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

import static com.facebook.presto.verifier.framework.ClusterType.CONTROL;
import static com.facebook.presto.verifier.framework.ClusterType.TEST;
import static com.google.common.base.Preconditions.checkState;
import static java.util.Objects.requireNonNull;

public enum QueryStage
{
    CONTROL_SETUP(CONTROL),
    CONTROL_MAIN(CONTROL),
    CONTROL_TEARDOWN(CONTROL),
    CONTROL_CHECKSUM(CONTROL),

    TEST_SETUP(TEST),
    TEST_MAIN(TEST),
    TEST_TEARDOWN(TEST),
    TEST_CHECKSUM(CONTROL),

    REWRITE(CONTROL),
    DESCRIBE(CONTROL),
    DETERMINISM_ANALYSIS_SETUP(CONTROL),
    DETERMINISM_ANALYSIS_MAIN(CONTROL),
    DETERMINISM_ANALYSIS_CHECKSUM(CONTROL),

    // Running Presto query to fetch the source queries to be verified
    SOURCE(CONTROL);

    private final ClusterType targetCluster;

    QueryStage(ClusterType targetCluster)
    {
        this.targetCluster = requireNonNull(targetCluster, "targetCluster is null");
    }

    public ClusterType getTargetCluster()
    {
        return targetCluster;
    }

    public boolean isSetup()
    {
        return this == CONTROL_SETUP || this == TEST_SETUP;
    }

    public boolean isMain()
    {
        return this == CONTROL_MAIN || this == TEST_MAIN;
    }

    public boolean isTeardown()
    {
        return this == CONTROL_TEARDOWN || this == TEST_TEARDOWN;
    }

    public static QueryStage forSetup(ClusterType cluster)
    {
        checkState(cluster == CONTROL || cluster == TEST, "Invalid cluster: %s", cluster);
        return cluster == CONTROL ? CONTROL_SETUP : TEST_SETUP;
    }

    public static QueryStage forMain(ClusterType cluster)
    {
        checkState(cluster == CONTROL || cluster == TEST, "Invalid cluster: %s", cluster);
        return cluster == CONTROL ? CONTROL_MAIN : TEST_MAIN;
    }

    public static QueryStage forTeardown(ClusterType cluster)
    {
        checkState(cluster == CONTROL || cluster == TEST, "Invalid cluster: %s", cluster);
        return cluster == CONTROL ? CONTROL_TEARDOWN : TEST_TEARDOWN;
    }
}
