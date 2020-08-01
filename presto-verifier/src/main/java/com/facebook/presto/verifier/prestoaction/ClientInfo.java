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
package com.facebook.presto.verifier.prestoaction;

import com.fasterxml.jackson.annotation.JsonProperty;
import com.fasterxml.jackson.core.JsonProcessingException;
import com.fasterxml.jackson.databind.ObjectMapper;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ClientInfo
{
    private static final String CLIENT_INFO_TYPE = "VERIFIER";

    private final String testId;
    private final Optional<String> testName;
    private final String sourceQueryName;
    private final String suite;

    public ClientInfo(String testId, Optional<String> testName, String sourceQueryName, String suite)
    {
        this.testId = requireNonNull(testId, "testId is null");
        this.testName = requireNonNull(testName, "testName is null");
        this.sourceQueryName = requireNonNull(sourceQueryName, "sourceQueryName is null");
        this.suite = requireNonNull(suite, "suite is null");
    }

    @JsonProperty
    public String getType()
    {
        return CLIENT_INFO_TYPE;
    }

    @JsonProperty
    public String getTestId()
    {
        return testId;
    }

    @JsonProperty
    public Optional<String> getTestName()
    {
        return testName;
    }

    @JsonProperty
    public String getSourceQueryName()
    {
        return sourceQueryName;
    }

    @JsonProperty
    public String getSuite()
    {
        return suite;
    }

    public String serialize()
    {
        try {
            return new ObjectMapper().writeValueAsString(this);
        }
        catch (JsonProcessingException e) {
            throw new RuntimeException(e);
        }
    }
}
