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
package com.facebook.presto.hive;

import com.facebook.presto.spi.PrestoException;

import java.util.Optional;

import static com.facebook.presto.spi.StandardErrorCode.ALREADY_EXISTS;
import static java.lang.String.format;

public class TableConstraintAlreadyExistsException
        extends PrestoException
{
    private final Optional<String> constraintName;

    public TableConstraintAlreadyExistsException(Optional<String> constraintName)
    {
        this(constraintName, format("Constraint already exists: '%s'", constraintName.get()));
    }

    public TableConstraintAlreadyExistsException(Optional<String> constraintName, String message)
    {
        this(constraintName, message, null);
    }

    public TableConstraintAlreadyExistsException(Optional<String> constraintName, String message, Throwable cause)
    {
        super(ALREADY_EXISTS, message, cause);
        this.constraintName = constraintName;
    }

    public Optional<String> getConstraintName()
    {
        return constraintName;
    }
}
