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
package io.prestosql.plugin.accumulo;

import io.prestosql.spi.ErrorCode;
import io.prestosql.spi.ErrorCodeSupplier;
import io.prestosql.spi.ErrorType;

import static io.prestosql.spi.ErrorType.EXTERNAL;

public enum AccumuloErrorCode
        implements ErrorCodeSupplier
{
    // Thrown when an Accumulo error is caught that we were not expecting,
    // such as when a create table operation fails (even though we know it will succeed due to our validation steps)
    UNEXPECTED_ACCUMULO_ERROR(1, EXTERNAL),

    // Thrown when a ZooKeeper error is caught due to a failed operation
    ZOOKEEPER_ERROR(2, EXTERNAL),

    // Thrown when a serialization error occurs when reading/writing data from/to Accumulo
    IO_ERROR(3, EXTERNAL),

    // Thrown when a table that is expected to exist does not exist
    ACCUMULO_TABLE_DNE(4, EXTERNAL),

    // Thrown when a table that is *not* expected to exist, does exist
    ACCUMULO_TABLE_EXISTS(5, EXTERNAL),

    // Thrown when an attempt to start/stop MiniAccumuloCluster fails (testing only)
    MINI_ACCUMULO(6, EXTERNAL);

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
