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
package com.teradata.presto.functions.dateformat;

import org.joda.time.format.DateTimeFormatterBuilder;

/**
 * Token binds string representation of a date format from Teradata
 * to a particular DateTimeFormatter
 *
 * For example if we encounter "YYYY" string as date format, we must
 * call "appendYear" on DateTimeFormatterBuilder
 */
public interface Token
{
    /**
     * @return String representation of this token in Teradata.
     */
    public String representation();

    /**
     * @param builder
     *
     * Append action associated with this token to the builder
     */
    void appendTo(DateTimeFormatterBuilder builder);
}
