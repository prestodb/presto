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
package com.facebook.presto.sql;

import com.facebook.airlift.configuration.Config;
import com.facebook.airlift.configuration.ConfigDescription;
import com.facebook.presto.common.type.TimeZoneKey;

import javax.annotation.Nullable;
import javax.validation.constraints.NotNull;

import java.util.Optional;

public class SqlEnvironmentConfig
{
    private Optional<TimeZoneKey> forcedSessionTimeZone = Optional.empty();

    @NotNull
    public Optional<TimeZoneKey> getForcedSessionTimeZone()
    {
        return forcedSessionTimeZone;
    }

    @Config("sql.forced-session-time-zone")
    @ConfigDescription("User session time zone overriding value sent by client")
    public SqlEnvironmentConfig setForcedSessionTimeZone(@Nullable String timeZoneId)
    {
        this.forcedSessionTimeZone = Optional.ofNullable(timeZoneId)
                .map(TimeZoneKey::getTimeZoneKey);
        return this;
    }
}
