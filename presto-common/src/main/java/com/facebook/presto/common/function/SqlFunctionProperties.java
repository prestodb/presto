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
package com.facebook.presto.common.function;

import com.facebook.presto.common.type.TimeZoneKey;

import java.util.Locale;
import java.util.Objects;

import static java.util.Objects.requireNonNull;

public class SqlFunctionProperties
{
    private final boolean parseDecimalLiteralAsDouble;
    private final boolean legacyRowFieldOrdinalAccessEnabled;
    private final boolean legacyTypeCoercionWarningEnabled;
    private final TimeZoneKey timeZoneKey;
    private final boolean legacyTimestamp;
    private final boolean legacyMapSubscript;
    private final long sessionStartTime;
    private final Locale sessionLocale;
    private final String sessionUser;

    private SqlFunctionProperties(
            boolean parseDecimalLiteralAsDouble,
            boolean legacyRowFieldOrdinalAccessEnabled,
            boolean legacyTypeCoercionWarningEnabled,
            TimeZoneKey timeZoneKey,
            boolean legacyTimestamp,
            boolean legacyMapSubscript,
            long sessionStartTime,
            Locale sessionLocale,
            String sessionUser)
    {
        this.parseDecimalLiteralAsDouble = parseDecimalLiteralAsDouble;
        this.legacyRowFieldOrdinalAccessEnabled = legacyRowFieldOrdinalAccessEnabled;
        this.legacyTypeCoercionWarningEnabled = legacyTypeCoercionWarningEnabled;
        this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
        this.legacyTimestamp = legacyTimestamp;
        this.legacyMapSubscript = legacyMapSubscript;
        this.sessionStartTime = sessionStartTime;
        this.sessionLocale = requireNonNull(sessionLocale, "sessionLocale is null");
        this.sessionUser = requireNonNull(sessionUser, "sessionUser is null");
    }

    public boolean isParseDecimalLiteralAsDouble()
    {
        return parseDecimalLiteralAsDouble;
    }

    public boolean isLegacyRowFieldOrdinalAccessEnabled()
    {
        return legacyRowFieldOrdinalAccessEnabled;
    }

    public boolean isLegacyTypeCoercionWarningEnabled()
    {
        return legacyTypeCoercionWarningEnabled;
    }

    public TimeZoneKey getTimeZoneKey()
    {
        return timeZoneKey;
    }

    @Deprecated
    public boolean isLegacyTimestamp()
    {
        return legacyTimestamp;
    }

    public boolean isLegacyMapSubscript()
    {
        return legacyMapSubscript;
    }

    public long getSessionStartTime()
    {
        return sessionStartTime;
    }

    public Locale getSessionLocale()
    {
        return sessionLocale;
    }

    public String getSessionUser()
    {
        return sessionUser;
    }

    @Override
    public boolean equals(Object o)
    {
        if (this == o) {
            return true;
        }
        if (!(o instanceof SqlFunctionProperties)) {
            return false;
        }
        SqlFunctionProperties that = (SqlFunctionProperties) o;
        return Objects.equals(parseDecimalLiteralAsDouble, that.parseDecimalLiteralAsDouble) &&
                Objects.equals(legacyRowFieldOrdinalAccessEnabled, that.legacyRowFieldOrdinalAccessEnabled) &&
                Objects.equals(timeZoneKey, that.timeZoneKey) &&
                Objects.equals(legacyTimestamp, that.legacyTimestamp) &&
                Objects.equals(legacyMapSubscript, that.legacyMapSubscript);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(parseDecimalLiteralAsDouble, legacyRowFieldOrdinalAccessEnabled, timeZoneKey, legacyTimestamp, legacyMapSubscript);
    }

    public static Builder builder()
    {
        return new Builder();
    }

    public static class Builder
    {
        private boolean parseDecimalLiteralAsDouble;
        private boolean legacyRowFieldOrdinalAccessEnabled;
        private boolean legacyTypeCoercionWarningEnabled;
        private TimeZoneKey timeZoneKey;
        private boolean legacyTimestamp;
        private boolean legacyMapSubscript;
        private long sessionStartTime;
        private Locale sessionLocale;
        private String sessionUser;

        private Builder() {}

        public Builder setParseDecimalLiteralAsDouble(boolean parseDecimalLiteralAsDouble)
        {
            this.parseDecimalLiteralAsDouble = parseDecimalLiteralAsDouble;
            return this;
        }

        public Builder setLegacyRowFieldOrdinalAccessEnabled(boolean legacyRowFieldOrdinalAccessEnabled)
        {
            this.legacyRowFieldOrdinalAccessEnabled = legacyRowFieldOrdinalAccessEnabled;
            return this;
        }

        public Builder setLegacyTypeCoercionWarningEnabled(boolean legacyTypeCoercionWarningEnabled)
        {
            this.legacyTypeCoercionWarningEnabled = legacyTypeCoercionWarningEnabled;
            return this;
        }

        public Builder setTimeZoneKey(TimeZoneKey timeZoneKey)
        {
            this.timeZoneKey = requireNonNull(timeZoneKey, "timeZoneKey is null");
            return this;
        }

        public Builder setLegacyTimestamp(boolean legacyTimestamp)
        {
            this.legacyTimestamp = legacyTimestamp;
            return this;
        }

        public Builder setLegacyMapSubscript(boolean legacyMapSubscript)
        {
            this.legacyMapSubscript = legacyMapSubscript;
            return this;
        }

        public Builder setSessionStartTime(long sessionStartTime)
        {
            this.sessionStartTime = sessionStartTime;
            return this;
        }

        public Builder setSessionLocale(Locale sessionLocale)
        {
            this.sessionLocale = sessionLocale;
            return this;
        }

        public Builder setSessionUser(String sessionUser)
        {
            this.sessionUser = sessionUser;
            return this;
        }

        public SqlFunctionProperties build()
        {
            return new SqlFunctionProperties(parseDecimalLiteralAsDouble, legacyRowFieldOrdinalAccessEnabled, legacyTypeCoercionWarningEnabled, timeZoneKey, legacyTimestamp, legacyMapSubscript, sessionStartTime, sessionLocale, sessionUser);
        }
    }
}
