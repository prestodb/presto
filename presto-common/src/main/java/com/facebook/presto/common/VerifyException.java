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
package com.facebook.presto.common;

import javax.annotation.Nullable;

/**
 * This class is copied from com.google.common.base.VerifyException.
 */
public class VerifyException
        extends RuntimeException
{
    /**
     * Constructs a {@code VerifyException} with no message.
     */
    public VerifyException() {}

    /**
     * Constructs a {@code VerifyException} with the message {@code message}.
     */
    public VerifyException(@Nullable String message)
    {
        super(message);
    }

    /**
     * Constructs a {@code VerifyException} with the cause {@code cause} and a message that is {@code
     * null} if {@code cause} is null, and {@code cause.toString()} otherwise.
     *
     * @since 19.0
     */
    public VerifyException(@Nullable Throwable cause)
    {
        super(cause);
    }

    /**
     * Constructs a {@code VerifyException} with the message {@code message} and the cause {@code
     * cause}.
     *
     * @since 19.0
     */
    public VerifyException(@Nullable String message, @Nullable Throwable cause)
    {
        super(message, cause);
    }
}
