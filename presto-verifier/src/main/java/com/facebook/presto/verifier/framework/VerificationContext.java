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

import com.facebook.presto.jdbc.QueryStats;
import com.facebook.presto.verifier.event.FailureInfo;
import com.facebook.presto.verifier.framework.QueryOrigin.TargetCluster;

import java.util.EnumMap;
import java.util.LinkedHashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import static com.facebook.presto.verifier.framework.QueryOrigin.TargetCluster.CONTROL;
import static com.facebook.presto.verifier.framework.QueryOrigin.TargetCluster.TEST;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static com.google.common.collect.ImmutableList.toImmutableList;

public class VerificationContext
{
    private Map<TargetCluster, Set<QueryException>> failures = new EnumMap<>(TargetCluster.class);

    public VerificationContext()
    {
        failures.put(CONTROL, new LinkedHashSet<>());
        failures.put(TEST, new LinkedHashSet<>());
    }

    public void recordFailure(QueryException exception)
    {
        failures.get(exception.getQueryOrigin().getCluster()).add(exception);
    }

    public List<FailureInfo> getAllFailures(TargetCluster cluster)
    {
        return failures.get(cluster).stream()
                .map(exception -> new FailureInfo(
                        exception.getQueryOrigin().getStage(),
                        exception.getErrorCode(),
                        exception.getQueryStats().map(QueryStats::getQueryId),
                        getStackTraceAsString(exception)))
                .collect(toImmutableList());
    }
}
