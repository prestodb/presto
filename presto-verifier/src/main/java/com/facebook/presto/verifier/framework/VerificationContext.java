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
import com.google.common.collect.ImmutableSet;

import java.util.List;
import java.util.Optional;

import static com.google.common.base.Preconditions.checkState;
import static com.google.common.collect.ImmutableList.toImmutableList;
import static java.util.Objects.requireNonNull;

public class VerificationContext
{
    private String controlChecksumQueryId;
    private String controlChecksumQuery;
    private String testChecksumQueryId;
    private String testChecksumQuery;

    private ImmutableSet.Builder<QueryException> queryExceptions = ImmutableSet.builder();

    public Optional<String> getControlChecksumQueryId()
    {
        return Optional.ofNullable(controlChecksumQueryId);
    }

    public void setControlChecksumQueryId(String controlChecksumQueryId)
    {
        checkState(this.controlChecksumQueryId == null, "controlChecksumQueryId is already set");
        this.controlChecksumQueryId = requireNonNull(controlChecksumQueryId, "controlChecksumQueryId is null");
    }

    public Optional<String> getControlChecksumQuery()
    {
        return Optional.ofNullable(controlChecksumQuery);
    }

    public void setControlChecksumQuery(String controlChecksumQuery)
    {
        checkState(this.controlChecksumQuery == null, "controlChecksumQuery is already set");
        this.controlChecksumQuery = requireNonNull(controlChecksumQuery, "controlChecksumQuery is null");
    }

    public Optional<String> getTestChecksumQueryId()
    {
        return Optional.ofNullable(testChecksumQueryId);
    }

    public void setTestChecksumQueryId(String testChecksumQueryId)
    {
        checkState(this.testChecksumQueryId == null, "testChecksumQueryId is already set");
        this.testChecksumQueryId = requireNonNull(testChecksumQueryId, "testChecksumQueryId is null");
    }

    public Optional<String> getTestChecksumQuery()
    {
        return Optional.ofNullable(testChecksumQuery);
    }

    public void setTestChecksumQuery(String testChecksumQuery)
    {
        checkState(this.testChecksumQuery == null, "testChecksumQuery is already set");
        this.testChecksumQuery = requireNonNull(testChecksumQuery, "testChecksumQuery is null");
    }

    public void addException(QueryException exception)
    {
        queryExceptions.add(exception);
    }

    public List<QueryFailure> getQueryFailures()
    {
        return queryExceptions.build().stream()
                .map(QueryException::toQueryFailure)
                .collect(toImmutableList());
    }
}
