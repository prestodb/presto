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
package com.facebook.presto.spi.transaction;

import com.facebook.drift.annotations.ThriftEnum;
import com.facebook.drift.annotations.ThriftEnumValue;
import com.facebook.presto.spi.PrestoException;

import static com.facebook.presto.spi.StandardErrorCode.UNSUPPORTED_ISOLATION_LEVEL;
import static java.lang.String.format;

@ThriftEnum
public enum IsolationLevel
{
    SERIALIZABLE(0),
    REPEATABLE_READ(1),
    READ_COMMITTED(2),
    READ_UNCOMMITTED(3);

    private final int value;

    public boolean meetsRequirementOf(IsolationLevel requirement)
    {
        switch (this) {
            case READ_UNCOMMITTED:
                return requirement == READ_UNCOMMITTED;

            case READ_COMMITTED:
                return requirement == READ_UNCOMMITTED ||
                        requirement == READ_COMMITTED;

            case REPEATABLE_READ:
                return requirement == READ_UNCOMMITTED ||
                        requirement == READ_COMMITTED ||
                        requirement == REPEATABLE_READ;

            case SERIALIZABLE:
                return true;
        }

        throw new AssertionError("Unhandled isolation level: " + this);
    }

    IsolationLevel(int value)
    {
        this.value = value;
    }

    @ThriftEnumValue
    public int getValue()
    {
        return value;
    }

    @Override
    public String toString()
    {
        return name().replace('_', ' ');
    }

    public static void checkConnectorSupports(IsolationLevel supportedLevel, IsolationLevel requestedLevel)
    {
        if (!supportedLevel.meetsRequirementOf(requestedLevel)) {
            throw new PrestoException(UNSUPPORTED_ISOLATION_LEVEL, format("Connector supported isolation level %s does not meet requested isolation level %s", supportedLevel, requestedLevel));
        }
    }
}
