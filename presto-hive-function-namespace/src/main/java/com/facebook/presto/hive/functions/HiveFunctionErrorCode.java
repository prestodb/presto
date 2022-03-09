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

package com.facebook.presto.hive.functions;

import com.facebook.presto.common.type.Type;
import com.facebook.presto.common.type.TypeSignature;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.ErrorCodeSupplier;
import com.facebook.presto.spi.ErrorType;
import com.facebook.presto.spi.PrestoException;
import org.apache.hadoop.hive.serde2.objectinspector.ObjectInspector;

import static com.facebook.presto.spi.ErrorType.EXTERNAL;
import static com.facebook.presto.spi.StandardErrorCode.FUNCTION_NOT_FOUND;
import static java.lang.String.format;

public enum HiveFunctionErrorCode
        implements ErrorCodeSupplier
{
    HIVE_FUNCTION_UNSUPPORTED_HIVE_TYPE(0, EXTERNAL),
    HIVE_FUNCTION_UNSUPPORTED_PRESTO_TYPE(1, EXTERNAL),
    HIVE_FUNCTION_UNSUPPORTED_FUNCTION_TYPE(2, EXTERNAL),
    HIVE_FUNCTION_INITIALIZATION_ERROR(3, EXTERNAL),
    HIVE_FUNCTION_EXECUTION_ERROR(4, EXTERNAL),
    /**/;

    private static final int ERROR_CODE_MASK = 0x0110_0000;

    private final ErrorCode errorCode;

    HiveFunctionErrorCode(int code, ErrorType type)
    {
        errorCode = new ErrorCode(code + ERROR_CODE_MASK, name(), type);
    }

    @Override
    public ErrorCode toErrorCode()
    {
        return errorCode;
    }

    public static PrestoException unsupportedType(Type type)
    {
        return new PrestoException(HIVE_FUNCTION_UNSUPPORTED_PRESTO_TYPE, "Unsupported Presto type " + type);
    }

    public static PrestoException unsupportedType(TypeSignature type)
    {
        return new PrestoException(HIVE_FUNCTION_UNSUPPORTED_PRESTO_TYPE, "Unsupported Presto type " + type);
    }

    public static PrestoException unsupportedType(ObjectInspector inspector)
    {
        return new PrestoException(HIVE_FUNCTION_UNSUPPORTED_HIVE_TYPE, "Unsupported Hive type " + inspector);
    }

    public static PrestoException unsupportedFunctionType(Class<?> cls)
    {
        return new PrestoException(HIVE_FUNCTION_UNSUPPORTED_FUNCTION_TYPE,
                format("Unsupported function type %s / %s", cls.getName(), cls.getSuperclass().getName()));
    }

    public static PrestoException unsupportedFunctionType(Class<?> cls, Throwable t)
    {
        return new PrestoException(HIVE_FUNCTION_UNSUPPORTED_FUNCTION_TYPE,
                format("Unsupported function type %s / %s", cls.getName(), cls.getSuperclass().getName()), t);
    }

    public static PrestoException functionNotFound(String name)
    {
        return new PrestoException(FUNCTION_NOT_FOUND, format("Function %s not registered", name));
    }

    public static PrestoException initializationError(Throwable t)
    {
        return new PrestoException(HIVE_FUNCTION_INITIALIZATION_ERROR, t);
    }

    public static PrestoException executionError(Throwable t)
    {
        return new PrestoException(HIVE_FUNCTION_EXECUTION_ERROR, t);
    }
}
