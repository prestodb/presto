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
package com.facebook.presto.spi;

public abstract class NotFoundException
        extends PrestoException
{
    protected NotFoundException()
    {
        this(null, null);
    }

    protected NotFoundException(String message)
    {
        this(message, null);
    }

    protected NotFoundException(Throwable cause)
    {
        this(null, cause);
    }

    protected NotFoundException(String message, Throwable cause)
    {
        super(StandardErrorCode.NOT_FOUND, message, cause);
    }

    protected NotFoundException(String message, Throwable cause, boolean enableSuppression, boolean writableStackTrace)
    {
        super(StandardErrorCode.NOT_FOUND, message, cause, enableSuppression, writableStackTrace);
    }
}
