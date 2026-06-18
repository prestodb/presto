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
package com.facebook.presto.sql.analyzer;

import java.util.List;

import static com.facebook.presto.sql.analyzer.SemanticErrorCode.EXCEPTIONS_WHEN_RESOLVING_FUNCTIONS;
import static java.lang.String.format;

public class SignatureMatchingException
        extends SemanticException
{
    public SignatureMatchingException(
            String prefix,
            List<SemanticException> failedExceptions)
    {
        super(EXCEPTIONS_WHEN_RESOLVING_FUNCTIONS, formatMessage(prefix, failedExceptions));
    }

    private static String formatMessage(String formatString, List<SemanticException> failedExceptions)
    {
        StringBuilder sb = new StringBuilder(formatString).append("\n");
        for (int i = 0; i < failedExceptions.size(); i++) {
            sb.append(format(" Exception %d: %s%n", i + 1, failedExceptions.get(i).getMessage()));
        }
        return sb.toString();
    }
}
