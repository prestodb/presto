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
import com.facebook.presto.nativeworker.PrestoNativeQueryRunnerUtils;
import com.facebook.presto.testing.ExpectedQueryRunner;
import com.facebook.presto.testing.QueryRunner;
import com.facebook.presto.tests.AbstractTestQueryFramework;
import org.intellij.lang.annotations.Language;
import org.testng.annotations.Test;

public class TestPrestoNativeGeospatial
        extends AbstractTestQueryFramework
{
    @Override
    protected QueryRunner createQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.nativeHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Override
    protected ExpectedQueryRunner createExpectedQueryRunner()
            throws Exception
    {
        return PrestoNativeQueryRunnerUtils.javaHiveQueryRunnerBuilder()
                .setAddStorageFormatToPath(true)
                .build();
    }

    @Test
    public void setSessionNativeUseVeloxGeospatialJoin()
    {
        @Language("SQL") String query = "WITH regions(name, geom) AS ( VALUES" +
                "('A', ST_GeometryFromText('POLYGON ((0 0, 0 5, 5 5, 5 0, 0 0))'))," +
                "('B', ST_GeometryFromText('POLYGON ((5 0, 5 5, 10 5, 10 0, 5 0))')))," +
                "points(id, geom) AS ( VALUES ('P1', ST_Point(1.0, 1.0)), ('P2', ST_Point(6.0, 1.0))," +
                "('P3', ST_Point(8.0, 4.0))) SELECT p.id, r.name FROM points p LEFT JOIN regions r ON ST_Within(p.geom, r.geom)";
        // Run the query with the session property native_use_velox_geospatial_join which defaults to true.
        assertQuery(query);
        // Set the session property native_use_velox_geospatial_join to false and run again.
        Session actualSession = Session.builder(getSession())
                .setSystemProperty("native_use_velox_geospatial_join", "false")
                .build();
        assertQuery(actualSession, query);
    }
}
