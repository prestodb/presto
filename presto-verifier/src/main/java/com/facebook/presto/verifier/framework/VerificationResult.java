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

public class VerificationResult
{
    private final String controlChecksumQueryId;
    private final String testChecksumQueryId;
    private final String controlChecksumQuery;
    private final String testChecksumQuery;
    private final MatchResult matchResult;

    public VerificationResult(
            String controlChecksumQueryId,
            String testChecksumQueryId,
            String controlChecksumQuery,
            String testChecksumQuery,
            MatchResult matchResult)
    {
        this.controlChecksumQueryId = requireNonNull(controlChecksumQueryId, "controlChecksumQueryId is null");
        this.testChecksumQueryId = requireNonNull(testChecksumQueryId, "testChecksumQueryId is null");
        this.controlChecksumQuery = requireNonNull(controlChecksumQuery, "controlChecksumQuery is null");
        this.testChecksumQuery = requireNonNull(testChecksumQuery, "testChecksumQuery is null");
        this.matchResult = requireNonNull(matchResult, "matchResult is null");
    }

    public String getControlChecksumQueryId()
    {
        return controlChecksumQueryId;
    }

    public String getControlChecksumQuery()
    {
        return controlChecksumQuery;
    }

    public String getTestChecksumQueryId()
    {
        return testChecksumQueryId;
    }

    public String getTestChecksumQuery()
    {
        return testChecksumQuery;
    }

    public MatchResult getMatchResult()
    {
        return matchResult;
    }
}
