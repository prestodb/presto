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
package com.facebook.presto.client;

import static com.google.common.base.Preconditions.checkNotNull;

public class Failure
        extends RuntimeException
{
    private final String type;

    Failure(String type, String message, Failure cause)
    {
        super(message, cause, true, true);
        this.type = checkNotNull(type, "type is null");
    }

    public String getType()
    {
        return type;
    }

    @Override
    public String toString()
    {
        String message = getMessage();
        if (message != null) {
            return type + ": " + message;
        }
        return type;
    }
}
