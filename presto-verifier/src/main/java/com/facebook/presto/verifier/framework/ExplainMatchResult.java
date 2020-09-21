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

import static java.util.Objects.requireNonNull;

public class ExplainMatchResult
        implements MatchResult
{
    public enum MatchType
    {
        MATCH,
        STRUCTURE_MISMATCH,
        DETAILS_MISMATCH,
    }

    private final MatchType matchType;

    public ExplainMatchResult(MatchType matchType)
    {
        this.matchType = requireNonNull(matchType, "matchType is null");
    }

    @Override
    public boolean isMatched()
    {
        return true;
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
    public String getReport()
    {
        return matchType.name();
    }
}
