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
package com.facebook.presto.cli;

import com.facebook.presto.client.ClientSession;

import java.io.PrintWriter;
import java.io.StringWriter;

public class ErrorMessages
{
    private ErrorMessages()
    {}

    public static String createErrorMessage(Throwable throwable, ClientSession session)
    {
        StringBuilder builder = new StringBuilder();
        builder.append("Error running command: " + throwable.getMessage());
        if (session.isDebug()) {
            builder.append(getStackTraceString(throwable));
        }
        return builder.toString();
    }

    private static String getStackTraceString(Throwable throwable)
    {
        StringWriter errors = new StringWriter();
        throwable.printStackTrace(new PrintWriter(errors));
        return errors.toString();
    }
}
