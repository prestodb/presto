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
package com.facebook.presto.tests;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import org.testng.annotations.Test;

public abstract class AbstractTestDateTimeScalarFunctions
        extends AbstractTestQueryFramework
{
    protected Session getSessionWithTimeZone(String timeZone)
    {
        return Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey(timeZone))
                .build();
    }

    @Test
    public void testLocalTime()
    {
        Session session = getSessionWithTimeZone("America/Chicago");
        assertQuerySucceeds(session, "SELECT localtime IS NOT NULL");
    }

    @Test
    public void testLocalTimestamp()
    {
        Session session = getSessionWithTimeZone("America/Chicago");
        assertQuerySucceeds(session, "SELECT localtimestamp IS NOT NULL");
    }

    @Test
    public void testCurrentTime()
    {
        Session session = getSessionWithTimeZone("America/Chicago");
        assertQuerySucceeds(session, "SELECT current_time IS NOT NULL");
    }

    @Test
    public void testCurrentTimestamp()
    {
        Session session = getSessionWithTimeZone("America/Chicago");
        assertQuerySucceeds(session, "SELECT current_timestamp IS NOT NULL");
    }

    @Test
    public void testCurrentTimezone()
    {
        Session session = getSessionWithTimeZone("America/Chicago");
        assertQuerySucceeds(session, "SELECT current_timezone() IS NOT NULL");
    }
}
