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
package com.facebook.presto;

import com.facebook.presto.execution.Failure;
import com.facebook.presto.spi.ErrorCode;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.sql.analyzer.SemanticException;
import com.facebook.presto.sql.parser.ParsingException;

import javax.annotation.Nullable;

import static com.facebook.presto.spi.StandardErrorCode.INTERNAL_ERROR;
import static com.facebook.presto.spi.StandardErrorCode.SYNTAX_ERROR;

public final class ErrorCodes
{
    private ErrorCodes()
    {
    }

    @Nullable
    public static ErrorCode toErrorCode(@Nullable Throwable throwable)
    {
        if (throwable == null) {
            return null;
        }

        if (throwable instanceof PrestoException) {
            return ((PrestoException) throwable).getErrorCode();
        }
        if (throwable instanceof Failure && ((Failure) throwable).getErrorCode() != null) {
            return ((Failure) throwable).getErrorCode();
        }
        if (throwable instanceof ParsingException || throwable instanceof SemanticException) {
            return SYNTAX_ERROR.toErrorCode();
        }
        if (throwable.getCause() != null) {
            return toErrorCode(throwable.getCause());
        }
        return INTERNAL_ERROR.toErrorCode();
    }
}
