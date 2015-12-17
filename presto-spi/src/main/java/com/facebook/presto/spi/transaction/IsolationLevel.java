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

import java.sql.Connection;

public enum IsolationLevel
{
    SERIALIZABLE,
    REPEATABLE_READ,
    READ_COMMITTED,
    READ_UNCOMMITTED;

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

    public static IsolationLevel fromJdbcValue(int value)
    {
        switch (value) {
            case Connection.TRANSACTION_READ_UNCOMMITTED:
                return READ_UNCOMMITTED;

            case Connection.TRANSACTION_READ_COMMITTED:
                return READ_COMMITTED;

            case Connection.TRANSACTION_REPEATABLE_READ:
                return REPEATABLE_READ;

            case Connection.TRANSACTION_SERIALIZABLE:
                return SERIALIZABLE;
        }

        throw new IllegalArgumentException("Unsupported isolation level value: " + value);
    }
}
