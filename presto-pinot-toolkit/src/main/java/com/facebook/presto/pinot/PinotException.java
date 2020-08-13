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
package com.facebook.presto.pinot;

import com.facebook.presto.spi.PrestoException;

import java.util.Optional;

import static java.util.Objects.requireNonNull;

public class PinotException
        extends PrestoException
{
    private final Optional<String> pinotQuery;
    private final PinotErrorCode pinotErrorCode;

    public PinotException(PinotErrorCode errorCode, Optional<String> pinotQuery, String message)
    {
        this(errorCode, pinotQuery, message, null);
    }

    public PinotException(PinotErrorCode pinotErrorCode, Optional<String> pinotQuery, String message, Throwable throwable)
    {
        super(requireNonNull(pinotErrorCode, "error code is null"), requireNonNull(message, "message is null"), throwable);
        this.pinotErrorCode = pinotErrorCode;
        this.pinotQuery = requireNonNull(pinotQuery, "pinot query is null");
    }

    public PinotErrorCode getPinotErrorCode()
    {
        return pinotErrorCode;
    }

    @Override
    public String getMessage()
    {
        String message = super.getMessage();
        if (pinotQuery.isPresent()) {
            message += " with pinot query \"" + pinotQuery.get() + "\"";
        }
        return message;
    }
}
