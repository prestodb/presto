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

import com.facebook.presto.common.ErrorType;
import com.facebook.presto.experimental.auto_gen.ThriftErrorType;

public class ThriftErrorTypeUtils
{
    private ThriftErrorTypeUtils() {}

    public static ErrorType toErrorType(ThriftErrorType thriftErrorType)
    {
        if (thriftErrorType == null) {
            return null;
        }
        return ErrorType.values()[thriftErrorType.getValue()];
    }

    public static ThriftErrorType fromErrorType(ErrorType errorType)
    {
        if (errorType == null) {
            return null;
        }
        return ThriftErrorType.findByValue(errorType.getCode());
    }
}
