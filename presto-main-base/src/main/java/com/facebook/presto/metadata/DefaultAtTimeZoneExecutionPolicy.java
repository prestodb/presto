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
package com.facebook.presto.metadata;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.FunctionsConfig;
import com.facebook.presto.sql.tree.QualifiedName;
import jakarta.inject.Inject;

import static com.facebook.presto.SystemSessionProperties.LEGACY_TIMESTAMP_WITH_TIMEZONE;
import static com.facebook.presto.common.type.TimestampType.TIMESTAMP;
import static com.facebook.presto.common.type.TimestampWithTimeZoneType.TIMESTAMP_WITH_TIME_ZONE;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_SESSION_PROPERTY;
import static java.lang.String.format;
import static java.util.Objects.requireNonNull;

public class DefaultAtTimeZoneExecutionPolicy
        implements AtTimeZoneExecutionPolicy
{
    private final boolean legacyTimestampWithTimezone;

    @Inject
    public DefaultAtTimeZoneExecutionPolicy(FunctionsConfig functionsConfig)
    {
        legacyTimestampWithTimezone = requireNonNull(functionsConfig, "functionsConfig is null").isLegacyTimestampWithTimezone();
    }

    @Override
    public QualifiedName resolveAtTimeZoneFunctionName(
            Type returnType,
            boolean nativeExecutionEnabled,
            boolean legacyTimestampWithTimezone)
    {
        Type resultType = requireNonNull(returnType, "returnType is null");
        boolean timestampResult = resultType.equals(TIMESTAMP) || resultType.equals(TIMESTAMP_WITH_TIME_ZONE);
        if (nativeExecutionEnabled && timestampResult && legacyTimestampWithTimezone != this.legacyTimestampWithTimezone) {
            throw new PrestoException(
                    INVALID_SESSION_PROPERTY,
                    format(
                            "%s is fixed to %s by the native server configuration, but the query requested %s",
                            LEGACY_TIMESTAMP_WITH_TIMEZONE,
                            this.legacyTimestampWithTimezone,
                            legacyTimestampWithTimezone));
        }
        return !nativeExecutionEnabled && resultType.equals(TIMESTAMP) ?
                QualifiedName.of("at_timezone_convert") :
                QualifiedName.of("at_timezone");
    }
}
