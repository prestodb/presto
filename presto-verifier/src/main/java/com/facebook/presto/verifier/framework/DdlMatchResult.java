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

import com.facebook.presto.sql.parser.ParsingException;

import java.util.Optional;

import static com.facebook.presto.verifier.framework.DdlMatchResult.MatchType.MATCH;
import static com.google.common.base.Throwables.getStackTraceAsString;
import static java.util.Objects.requireNonNull;

public class DdlMatchResult
        implements MatchResult
{
    public enum MatchType
    {
        MATCH,
        MISMATCH,
        CONTROL_NOT_PARSABLE,
        TEST_NOT_PARSABLE,
    }

    private final MatchType matchType;
    private final Optional<ParsingException> exception;
    private final String controlObject;
    private final String testObject;

    public DdlMatchResult(MatchType matchType, Optional<ParsingException> exception, String controlObject, String testObject)
    {
        this.matchType = requireNonNull(matchType, "matchType is null");
        this.exception = requireNonNull(exception, "exception is null");
        this.controlObject = requireNonNull(controlObject, "controlObject is null");
        this.testObject = requireNonNull(testObject, "testObject is null");
    }

    @Override
    public boolean isMatched()
    {
        return matchType == MATCH;
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
        StringBuilder message = new StringBuilder()
                .append(getMatchTypeName())
                .append("\n\n");
        exception.ifPresent(e -> message.append(getStackTraceAsString(e)).append("\n"));
        return message.append("Control:\n")
                .append(controlObject.trim())
                .append("\n\n")
                .append("Test:")
                .append(testObject.trim())
                .append("\n")
                .toString();
    }
}
