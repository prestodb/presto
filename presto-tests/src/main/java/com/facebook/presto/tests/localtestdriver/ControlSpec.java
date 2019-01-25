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
package com.facebook.presto.tests.localtestdriver;

import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;

import java.util.Map;
import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class ControlSpec
{
    private final Map<String, String> sessionProperties;
    private final Optional<String> backupDirectory;

    @JsonCreator
    public ControlSpec(
            @JsonProperty("sessionProperties") Map<String, String> sessionProperties,
            @JsonProperty("backupDirectory") Optional<String> backupDirectory)
    {
        this.sessionProperties = requireNonNull(sessionProperties, "sessionProperties is null");
        this.backupDirectory = requireNonNull(backupDirectory, "backupDirectory is null");
    }

    @JsonProperty
    public Map<String, String> getSessionProperties()
    {
        return sessionProperties;
    }

    @JsonProperty
    public Optional<String> getBackupDirectory()
    {
        return backupDirectory;
    }
}
