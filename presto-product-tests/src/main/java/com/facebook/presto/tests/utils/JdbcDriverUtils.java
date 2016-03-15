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
package com.facebook.presto.tests.utils;

import static com.teradata.tempto.query.QueryExecutor.defaultQueryExecutor;

public class JdbcDriverUtils
{
    public static boolean usingFacebookJdbcDriver()
    {
        return getClassNameForJdbcDriver().equals("com.facebook.presto.jdbc.PrestoConnection");
    }

    public static boolean usingSimbaJdbcDriver()
    {
        String className = getClassNameForJdbcDriver();
        return  className.equals("com.teradata.jdbc.jdbc4.S4Connection") ||
                className.equals("com.teradata.jdbc.jdbc41.S41Connection") ||
                className.equals("com.teradata.jdbc.jdbc42.S42Connection");
    }

    public static boolean usingSimbaJdbc4Driver()
    {
        return getClassNameForJdbcDriver().contains("jdbc4.");
    }

    private static String getClassNameForJdbcDriver()
    {
        return defaultQueryExecutor().getConnection().getClass().getCanonicalName();
    }

    private JdbcDriverUtils() {}
}
