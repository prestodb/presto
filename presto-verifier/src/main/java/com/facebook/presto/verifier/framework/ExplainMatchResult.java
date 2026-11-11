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

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ExplainMatchResult
        implements MatchResult
{
    public enum MatchType
    {
        MATCH,
        STRUCTURE_MISMATCH,
        DETAILS_MISMATCH,
        SNAPSHOT_DOES_NOT_EXIST,
    }

    private final MatchType matchType;
    private final Optional<String> mismatchDetails;

    public ExplainMatchResult(MatchType matchType)
    {
        this(matchType, Optional.empty());
    }

    public ExplainMatchResult(MatchType matchType, Optional<String> mismatchDetails)
    {
        this.matchType = requireNonNull(matchType, "matchType is null");
        this.mismatchDetails = requireNonNull(mismatchDetails, "mismatchDetails is null");
    }

    /**
     * A structure mismatch fails verification like a result mismatch does. A details-only
     * mismatch (estimates or operator details) does not.
     */
    @Override
    public boolean isMatched()
    {
        return matchType != MatchType.STRUCTURE_MISMATCH;
    }

    public MatchType getMatchType()
    {
        return matchType;
    }

    @Override
    public String getDataType()
    {
        return "EXPLAIN";
    }

    @Override
    public String getMatchTypeName()
    {
        return matchType.name();
    }

    @Override
    public boolean isMismatchPossiblyCausedByNonDeterminism()
    {
        return false;
    }

    @Override
    public boolean isMismatchPossiblyCausedByReuseOutdatedTable()
    {
        return false;
    }

    @Override
    public String getReport()
    {
        return matchType.name() + mismatchDetails.map(details -> "\n" + details).orElse("");
    }
}
