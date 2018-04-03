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
import com.facebook.presto.sql.planner.iterative.rule.TransformSpatialPredicates.TransformSpatialPredicateToLeftJoin;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.spatialLeftJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder.expression;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;

public class TestTransformSpatialPredicateToLeftJoin
        extends BaseRuleTest
{
    public TestTransformSpatialPredicateToLeftJoin()
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
                                p.values(p.symbol("b")),
                                expression("ST_Contains(ST_GeometryFromText('POLYGON ...'), b)")))
                .doesNotFire();

        // symbols
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("a")),
                                p.values(p.symbol("b")),
                                expression("ST_Contains(a, b)")))
                .doesNotFire();

        // OR operand
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt", VARCHAR), p.symbol("name_1")),
                                p.values(p.symbol("point", GEOMETRY), p.symbol("name_2")),
                                expression("ST_Contains(ST_GeometryFromText(wkt), point) OR name_1 != name_2")))
                .doesNotFire();

        // NOT operator
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt", VARCHAR), p.symbol("name_1")),
                                p.values(p.symbol("point", GEOMETRY), p.symbol("name_2")),
                                expression("NOT ST_Contains(ST_GeometryFromText(wkt), point)")))
                .doesNotFire();

        // ST_Distance(...) > r
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("a", GEOMETRY)),
                                p.values(p.symbol("b", GEOMETRY)),
                                expression("ST_Distance(a, b) > 5")))
                .doesNotFire();
    }

    @Test
    public void testPushDownFirstArgument()
    {
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt", VARCHAR)),
                                p.values(p.symbol("point", GEOMETRY)),
                                expression("ST_Contains(ST_GeometryFromText(wkt), point)")))
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, point)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0))),
                                values(ImmutableMap.of("point", 0))));

        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt", VARCHAR)),
                                p.values(),
                                expression("ST_Contains(ST_GeometryFromText(wkt), ST_Point(0, 0))")))
                .doesNotFire();
    }

    @Test
    public void testPushDownSecondArgument()
    {
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("polygon", GEOMETRY)),
                                p.values(p.symbol("lat"), p.symbol("lng")),
                                expression("ST_Contains(polygon, ST_Point(lng, lat))")))
                .matches(
                        spatialLeftJoin("ST_Contains(polygon, st_point)",
                                values(ImmutableMap.of("polygon", 0)),
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1)))));

        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(),
                                p.values(p.symbol("lat"), p.symbol("lng")),
                                expression("ST_Contains(ST_GeometryFromText('POLYGON ...'), ST_Point(lng, lat))")))
                .doesNotFire();
    }

    @Test
    public void testPushDownBothArguments()
    {
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt", VARCHAR)),
                                p.values(p.symbol("lat"), p.symbol("lng")),
                                expression("ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))")))
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
                                p.values(p.symbol("lat"), p.symbol("lng")),
                                p.values(p.symbol("wkt", VARCHAR)),
                                expression("ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))")))
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
                                p.values(p.symbol("wkt", VARCHAR), p.symbol("name_1")),
                                p.values(p.symbol("lat"), p.symbol("lng"), p.symbol("name_2")),
                                expression("name_1 != name_2 AND ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))")))
                .matches(
                        spatialLeftJoin("name_1 != name_2 AND ST_Contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0, "name_1", 1))),
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1, "name_2", 2)))));

        // Multiple spatial functions - only the first one is being processed
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.symbol("wkt1", VARCHAR), p.symbol("wkt2", VARCHAR)),
                                p.values(p.symbol("geometry1"), p.symbol("geometry2")),
                                expression("ST_Contains(ST_GeometryFromText(wkt1), geometry1) AND ST_Contains(ST_GeometryFromText(wkt2), geometry2)")))
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, geometry1) AND ST_Contains(ST_GeometryFromText(wkt2), geometry2)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt1)")), values(ImmutableMap.of("wkt1", 0, "wkt2", 1))),
                                values(ImmutableMap.of("geometry1", 0, "geometry2", 1))));
    }

    private RuleAssert assertRuleApplication()
    {
        return tester().assertThat(new TransformSpatialPredicateToLeftJoin(tester().getMetadata()));
    }
}
