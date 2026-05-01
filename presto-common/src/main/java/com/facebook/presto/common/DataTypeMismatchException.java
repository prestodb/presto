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

/**
 * Thrown when two operands have incompatible types, in modules (such as
 * {@code presto-common}) that cannot depend on {@code presto-spi}. {@link
 * com.facebook.presto.util.Failures#toErrorCode} translates this to
 * {@link com.facebook.presto.spi.StandardErrorCode#DATATYPE_MISMATCH}.
 */
public class DataTypeMismatchException
        extends RuntimeException
{
    public DataTypeMismatchException(String message)
    {
        super(message);
    }

    public DataTypeMismatchException(String message, Throwable cause)
    {
        super(message, cause);
    }
}
