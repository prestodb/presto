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
package com.facebook.presto.plugin.geospatial;

import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.ExtractSpatialJoins.ExtractSpatialLeftJoin;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY;
import static com.facebook.presto.plugin.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.spatialLeftJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.castToRowExpression;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;

public class TestExtractSpatialLeftJoin
        extends BaseRuleTest
{
    public TestExtractSpatialLeftJoin()
    {
        super(new GeoPlugin());
    }

    @Test
    public void testDoesNotFire()
    {
        // scalar expression
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(),
                                p.values(p.variable("b")),
                                castToRowExpression("ST_Contains(ST_GeometryFromText('POLYGON ...'), b)")))
                .doesNotFire();

        // OR operand
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt", VARCHAR), p.variable("name_1")),
                                p.values(p.variable("point", GEOMETRY), p.variable("name_2")),
                                castToRowExpression("ST_Contains(ST_GeometryFromText(wkt), point) OR name_1 != name_2")))
                .doesNotFire();

        // NOT operator
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt", VARCHAR), p.variable("name_1")),
                                p.values(p.variable("point", GEOMETRY), p.variable("name_2")),
                                castToRowExpression("NOT ST_Contains(ST_GeometryFromText(wkt), point)")))
                .doesNotFire();

        // ST_Distance(...) > r
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("a", GEOMETRY)),
                                p.values(p.variable("b", GEOMETRY)),
                                castToRowExpression("ST_Distance(a, b) > 5")))
                .doesNotFire();

        // SphericalGeography operand
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("a", SPHERICAL_GEOGRAPHY)),
                                p.values(p.variable("b", SPHERICAL_GEOGRAPHY)),
                                castToRowExpression("ST_Distance(a, b) < 5")))
                .doesNotFire();

        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("polygon", SPHERICAL_GEOGRAPHY)),
                                p.values(p.variable("point", SPHERICAL_GEOGRAPHY)),
                                castToRowExpression("ST_Contains(polygon, point)")))
                .doesNotFire();

        // to_spherical_geography() operand
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt", VARCHAR)),
                                p.values(p.variable("point", SPHERICAL_GEOGRAPHY)),
                                castToRowExpression("ST_Distance(to_spherical_geography(ST_GeometryFromText(wkt)), point) < 5")))
                .doesNotFire();

        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt", VARCHAR)),
                                p.values(p.variable("point", SPHERICAL_GEOGRAPHY)),
                                castToRowExpression("ST_Contains(to_spherical_geography(ST_GeometryFromText(wkt)), point)")))
                .doesNotFire();
    }

    @Test
    public void testConvertToSpatialJoin()
    {
        // symbols
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("a")),
                                p.values(p.variable("b")),
                                castToRowExpression("ST_Contains(a, b)")))
                .matches(
                        spatialLeftJoin("ST_Contains(a, b)",
                                values(ImmutableMap.of("a", 0)),
                                values(ImmutableMap.of("b", 0))));

        // AND
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("a"), p.variable("name_1")),
                                p.values(p.variable("b"), p.variable("name_2")),
                                castToRowExpression("name_1 != name_2 AND ST_Contains(a, b)")))
                .matches(
                        spatialLeftJoin("name_1 != name_2 AND ST_Contains(a, b)",
                                values(ImmutableMap.of("a", 0, "name_1", 1)),
                                values(ImmutableMap.of("b", 0, "name_2", 1))));

        // AND
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("a1"), p.variable("a2")),
                                p.values(p.variable("b1"), p.variable("b2")),
                                castToRowExpression("ST_Contains(a1, b1) AND ST_Contains(a2, b2)")))
                .matches(
                        spatialLeftJoin("ST_Contains(a1, b1) AND ST_Contains(a2, b2)",
                                values(ImmutableMap.of("a1", 0, "a2", 1)),
                                values(ImmutableMap.of("b1", 0, "b2", 1))));
    }

    @Test
    public void testPushDownFirstArgument()
    {
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt", VARCHAR)),
                                p.values(p.variable("point", GEOMETRY)),
                                castToRowExpression("ST_Contains(ST_GeometryFromText(wkt), point)")))
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, point)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0))),
                                values(ImmutableMap.of("point", 0))));

        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt", VARCHAR)),
                                p.values(),
                                castToRowExpression("ST_Contains(ST_GeometryFromText(wkt), ST_Point(0, 0))")))
                .doesNotFire();
    }

    @Test
    public void testPushDownSecondArgument()
    {
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("polygon", GEOMETRY)),
                                p.values(p.variable("lat"), p.variable("lng")),
                                castToRowExpression("ST_Contains(polygon, ST_Point(lng, lat))")))
                .matches(
                        spatialLeftJoin("ST_Contains(polygon, st_point)",
                                values(ImmutableMap.of("polygon", 0)),
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1)))));

        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(),
                                p.values(p.variable("lat"), p.variable("lng")),
                                castToRowExpression("ST_Contains(ST_GeometryFromText('POLYGON ...'), ST_Point(lng, lat))")))
                .doesNotFire();
    }

    @Test
    public void testPushDownBothArguments()
    {
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt", VARCHAR)),
                                p.values(p.variable("lat"), p.variable("lng")),
                                castToRowExpression("ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))")))
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0))),
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1)))));
    }

    @Test
    public void testPushDownOppositeOrder()
    {
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("lat"), p.variable("lng")),
                                p.values(p.variable("wkt", VARCHAR)),
                                castToRowExpression("ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))")))
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1))),
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0)))));
    }

    @Test
    public void testPushDownAnd()
    {
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt", VARCHAR), p.variable("name_1")),
                                p.values(p.variable("lat"), p.variable("lng"), p.variable("name_2")),
                                castToRowExpression("name_1 != name_2 AND ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))")))
                .matches(
                        spatialLeftJoin("name_1 != name_2 AND ST_Contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0, "name_1", 1))),
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1, "name_2", 2)))));

        // Multiple spatial functions - only the first one is being processed
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt1", VARCHAR), p.variable("wkt2", VARCHAR)),
                                p.values(p.variable("geometry1"), p.variable("geometry2")),
                                castToRowExpression("ST_Contains(ST_GeometryFromText(wkt1), geometry1) AND ST_Contains(ST_GeometryFromText(wkt2), geometry2)")))
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, geometry1) AND ST_Contains(ST_GeometryFromText(wkt2), geometry2)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt1)")), values(ImmutableMap.of("wkt1", 0, "wkt2", 1))),
                                values(ImmutableMap.of("geometry1", 0, "geometry2", 1))));
    }

    private RuleAssert assertRuleApplication()
    {
        RuleTester tester = tester();
        return tester().assertThat(new ExtractSpatialLeftJoin(tester.getMetadata(), tester.getSplitManager(), tester.getPageSourceManager(), tester.getSqlParser()));
    }
}
