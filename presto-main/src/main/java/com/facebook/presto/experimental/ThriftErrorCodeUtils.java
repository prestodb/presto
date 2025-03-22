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
package com.facebook.presto.experimental;

import com.facebook.presto.common.ErrorCode;
import com.facebook.presto.experimental.auto_gen.ThriftErrorCode;

import static com.facebook.presto.experimental.ThriftErrorTypeUtils.fromErrorType;
import static com.facebook.presto.experimental.ThriftErrorTypeUtils.toErrorType;

public class ThriftErrorCodeUtils
{
    private ThriftErrorCodeUtils() {}

    public static ErrorCode toErrorCode(ThriftErrorCode thriftErrorCode)
    {
        return new ErrorCode(thriftErrorCode.getCode(), thriftErrorCode.getName(), toErrorType(thriftErrorCode.getType()), thriftErrorCode.isRetriable());
    }

    public static ThriftErrorCode fromErrorCode(ErrorCode errorCode)
    {
        if (errorCode == null) {
            return null;
        }
        return new ThriftErrorCode(errorCode.getCode(),
                errorCode.getName(),
                fromErrorType(errorCode.getType()),
                errorCode.isRetriable());
    }
}
