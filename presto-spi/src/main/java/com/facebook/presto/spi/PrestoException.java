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

import com.facebook.presto.common.ErrorCode;

public class PrestoException
        extends RuntimeException
{
    private final ErrorCode errorCode;
    private final ErrorCodeSupplier errorCodeSupplier;
    private final String errorKey;
    private final Object[] args;

    public PrestoException(ErrorCodeSupplier errorCode, String message)
    {
        this(errorCode, message, null);
    }
    public PrestoException(String errorKey, ErrorCodeSupplier errorCodeSupplier, Object...args)
    {
        this(errorKey, errorCodeSupplier, null, args);
    }

    public PrestoException(ErrorCodeSupplier errorCode, Throwable throwable)
    {
        this(errorCode, null, throwable);
    }

    public PrestoException(ErrorCodeSupplier errorCodeSupplier, String message, Throwable cause)
    {
        super(message, cause);
        this.errorCode = errorCodeSupplier.toErrorCode();
        this.errorCodeSupplier = errorCodeSupplier;
        this.errorKey = null;
        this.args = null;
    }

    public PrestoException(String errorKey, ErrorCodeSupplier errorCodeSupplier, Throwable cause, Object...args)
    {
        super(cause);
        this.errorCodeSupplier = errorCodeSupplier;
        this.errorKey = errorKey;
        this.errorCode = errorCodeSupplier.toErrorCode();
        this.args = args;
    }

    public ErrorCode getErrorCode()
    {
        return errorCode;
    }

    public ErrorCodeSupplier getErrorCodeSupplier()
    {
        return errorCodeSupplier;
    }

    public String getErrorKey()
    {
        return errorKey;
    }

    public Object[] getArgs()
    {
        return args;
    }

    @Override
    public String getMessage()
    {
        String message = super.getMessage();
        if (message == null && getCause() != null) {
            message = getCause().getMessage();
        }
        if (message == null) {
            message = errorCode.getName();
        }
        return message;
    }
}
