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
package com.facebook.presto.nativetests;

import com.facebook.presto.Session;
import com.facebook.presto.common.type.TimeZoneKey;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import static java.lang.Boolean.parseBoolean;

public class TestPrestoNativeDateTimeScalarFunctions
        extends AbstractTestQueryFramework
{
    private String storageFormat;
    private boolean sidecarEnabled;
    private Session session;

    @BeforeClass
    @Override
    public void init()
            throws Exception
    {
        storageFormat = System.getProperty("storageFormat", "PARQUET");
        sidecarEnabled = parseBoolean(System.getProperty("sidecarEnabled", "true"));
        super.init();
        session =
        Session.builder(getSession())
                .setTimeZoneKey(TimeZoneKey.getTimeZoneKey("America/Chicago"))
                .build();
    }

    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        QueryRunner queryRunner = NativeTestsUtils.createNativeQueryRunner(storageFormat, sidecarEnabled);
        return queryRunner;
    }

    @Test
    public void testLocalTime()
    {
        assertQuerySucceeds(session, "SELECT localtime IS NOT NULL");
    }

    @Test
    public void testLocalTimestamp()
    {
        assertQuerySucceeds(session, "SELECT localtimestamp IS NOT NULL");
    }

    @Test
    public void testCurrentTime()
    {
        assertQuerySucceeds(session, "SELECT current_time IS NOT NULL");
    }

    @Test
    public void testCurrentTimestamp()
    {
        assertQuerySucceeds(session, "SELECT current_timestamp IS NOT NULL");
    }

    @Test
    public void testCurrentTimezone()
    {
        assertQuerySucceeds(session, "SELECT current_timezone() IS NOT NULL");
    }
}
