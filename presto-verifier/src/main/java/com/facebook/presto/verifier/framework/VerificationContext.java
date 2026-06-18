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

import com.facebook.presto.verifier.event.QueryFailure;

import java.util.HashSet;
import java.util.List;
import java.util.Set;

import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class VerificationContext
{
    private final String sourceQueryName;
    private final String suite;

    private final int resubmissionCount;
    private final Set<QueryException> queryExceptions;

    private VerificationContext(
            String sourceQueryName,
            String suite,
            int resubmissionCount,
            Set<QueryException> queryExceptions)
    {
        this.sourceQueryName = requireNonNull(sourceQueryName, "sourceQueryName is null");
        this.suite = requireNonNull(suite, "suite is null");
        this.resubmissionCount = resubmissionCount;
        this.queryExceptions = new HashSet<>(queryExceptions);
    }

    public static VerificationContext create(String sourceQueryName, String suite)
    {
        return new VerificationContext(sourceQueryName, suite, 0, new HashSet<>());
    }

    public static VerificationContext createForResubmission(VerificationContext existing)
    {
        return new VerificationContext(
                existing.sourceQueryName,
                existing.suite,
                existing.resubmissionCount + 1,
                existing.queryExceptions);
    }

    public String getSourceQueryName()
    {
        return sourceQueryName;
    }

    public String getSuite()
    {
        return suite;
    }

    public int getResubmissionCount()
    {
        return resubmissionCount;
    }

    public void addException(QueryException exception)
    {
        queryExceptions.add(exception);
    }

    public List<QueryFailure> getQueryFailures()
    {
        return queryExceptions.stream()
                .map(QueryException::toQueryFailure)
                .collect(toImmutableList());
    }
}
