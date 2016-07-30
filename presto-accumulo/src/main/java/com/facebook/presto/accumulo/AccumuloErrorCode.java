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
package com.facebook.presto.accumulo;

import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.ErrorType.USER_ERROR;

public enum AccumuloErrorCode
        implements ErrorCodeSupplier
{
    // Thrown when an internal Accumulo connector error occurs
    // Generally some assumption that turned out to be wrong
    // Helpful to identify errors during development
    INTERNAL_ERROR(0, ErrorType.INTERNAL_ERROR),

    // Thrown when a user tries to do an unsupported operation
    NOT_SUPPORTED(1, USER_ERROR),

    // Thrown when an Accumulo error is caught that we were not expecting,
    // such as when a create table operation fails (even though we know it will succeed due to our validation steps)
    UNEXPECTED_ACCUMULO_ERROR(2, EXTERNAL),

    // Thrown when a ZooKeeper error is caught due to a failed operation
    ZOOKEEPER_ERROR(3, EXTERNAL),

    // Thrown when a serialization error occurs when reading/writing data from/to Accumulo
    IO_ERROR(4, EXTERNAL),

    // Thrown when an error occurs in validating operations within the connector,
    // such as a user trying to create a table that already exists in Accumulo
    VALIDATION(5, USER_ERROR),

    // Thrown when a table that is expected to exist does not exist
    ACCUMULO_TABLE_DNE(6, EXTERNAL),

    // Thrown when a table that is *not* expected to exist, does exist
    ACCUMULO_TABLE_EXISTS(7, EXTERNAL),

    // Thrown when an attempt is made to create a view that already exists
    VIEW_ALREADY_EXISTS(8, EXTERNAL),

    // Thrown when an attempt is made to create a view that is the same name as an existing Presto table
    VIEW_IS_TABLE(9, EXTERNAL),

    // Thrown when a column is unable to be discovered
    COLUMN_NOT_FOUND(10, USER_ERROR),

    // Thrown when an attempt to start/stop MiniAccumuloCluster fails (testing only)
    MINI_ACCUMULO(11, EXTERNAL);

    private final ErrorCode errorCode;

    AccumuloErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + 0x0103_0000, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }
}
