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

import com.facebook.presto.sql.planner.iterative.rule.ExtractSpatialJoins.ExtractSpatialInnerJoin;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.Test;

import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY;
import static com.facebook.presto.spi.type.VarcharType.VARCHAR;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.expression;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.spatialJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.INNER;

public class TestExtractSpatialInnerJoin
        extends BaseRuleTest
{
    public TestExtractSpatialInnerJoin()
    {
        super(new GeoPlugin());
    }

    @Test
    public void testDoesNotFire()
    {
        // scalar expression
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("ST_Contains(ST_GeometryFromText('POLYGON ...'), b)"),
                                p.join(INNER,
                                        p.values(),
                                        p.values(p.symbol("b")))))
                .doesNotFire();

        // OR operand
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("ST_Contains(ST_GeometryFromText(wkt), point) OR name_1 != name_2"),
                                p.join(INNER,
                                        p.values(p.symbol("wkt", VARCHAR), p.symbol("name_1")),
                                        p.values(p.symbol("point", GEOMETRY), p.symbol("name_2")))))
                .doesNotFire();

        // NOT operator
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("NOT ST_Contains(ST_GeometryFromText(wkt), point)"),
                                p.join(INNER,
                                        p.values(p.symbol("wkt", VARCHAR), p.symbol("name_1")),
                                        p.values(p.symbol("point", GEOMETRY), p.symbol("name_2")))))
                .doesNotFire();

        // ST_Distance(...) > r
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("ST_Distance(a, b) > 5"),
                                p.join(INNER,
                                        p.values(p.symbol("a", GEOMETRY)),
                                        p.values(p.symbol("b", GEOMETRY)))))
                .doesNotFire();
    }

    @Test
    public void testDistanceQueries()
    {
        testSimpleDistanceQuery("ST_Distance(a, b) <= r", "ST_Distance(a, b) <= r");
        testSimpleDistanceQuery("ST_Distance(b, a) <= r", "ST_Distance(b, a) <= r");
        testSimpleDistanceQuery("r >= ST_Distance(a, b)", "ST_Distance(a, b) <= r");
        testSimpleDistanceQuery("r >= ST_Distance(b, a)", "ST_Distance(b, a) <= r");

        testSimpleDistanceQuery("ST_Distance(a, b) < r", "ST_Distance(a, b) < r");
        testSimpleDistanceQuery("ST_Distance(b, a) < r", "ST_Distance(b, a) < r");
        testSimpleDistanceQuery("r > ST_Distance(a, b)", "ST_Distance(a, b) < r");
        testSimpleDistanceQuery("r > ST_Distance(b, a)", "ST_Distance(b, a) < r");

        testSimpleDistanceQuery("ST_Distance(a, b) <= r AND name_a != name_b", "ST_Distance(a, b) <= r AND name_a != name_b");
        testSimpleDistanceQuery("r > ST_Distance(a, b) AND name_a != name_b", "ST_Distance(a, b) < r AND name_a != name_b");

        testRadiusExpressionInDistanceQuery("ST_Distance(a, b) <= decimal '1.2'", "ST_Distance(a, b) <= radius", "decimal '1.2'");
        testRadiusExpressionInDistanceQuery("ST_Distance(b, a) <= decimal '1.2'", "ST_Distance(b, a) <= radius", "decimal '1.2'");
        testRadiusExpressionInDistanceQuery("decimal '1.2' >= ST_Distance(a, b)", "ST_Distance(a, b) <= radius", "decimal '1.2'");
        testRadiusExpressionInDistanceQuery("decimal '1.2' >= ST_Distance(b, a)", "ST_Distance(b, a) <= radius", "decimal '1.2'");

        testRadiusExpressionInDistanceQuery("ST_Distance(a, b) < decimal '1.2'", "ST_Distance(a, b) < radius", "decimal '1.2'");
        testRadiusExpressionInDistanceQuery("ST_Distance(b, a) < decimal '1.2'", "ST_Distance(b, a) < radius", "decimal '1.2'");
        testRadiusExpressionInDistanceQuery("decimal '1.2' > ST_Distance(a, b)", "ST_Distance(a, b) < radius", "decimal '1.2'");
        testRadiusExpressionInDistanceQuery("decimal '1.2' > ST_Distance(b, a)", "ST_Distance(b, a) < radius", "decimal '1.2'");

        testRadiusExpressionInDistanceQuery("ST_Distance(a, b) <= decimal '1.2' AND name_a != name_b", "ST_Distance(a, b) <= radius AND name_a != name_b", "decimal '1.2'");
        testRadiusExpressionInDistanceQuery("decimal '1.2' > ST_Distance(a, b) AND name_a != name_b", "ST_Distance(a, b) < radius AND name_a != name_b", "decimal '1.2'");

        testRadiusExpressionInDistanceQuery("ST_Distance(a, b) <= 2 * r", "ST_Distance(a, b) <= radius", "2 * r");
        testRadiusExpressionInDistanceQuery("ST_Distance(b, a) <= 2 * r", "ST_Distance(b, a) <= radius", "2 * r");
        testRadiusExpressionInDistanceQuery("2 * r >= ST_Distance(a, b)", "ST_Distance(a, b) <= radius", "2 * r");
        testRadiusExpressionInDistanceQuery("2 * r >= ST_Distance(b, a)", "ST_Distance(b, a) <= radius", "2 * r");

        testRadiusExpressionInDistanceQuery("ST_Distance(a, b) < 2 * r", "ST_Distance(a, b) < radius", "2 * r");
        testRadiusExpressionInDistanceQuery("ST_Distance(b, a) < 2 * r", "ST_Distance(b, a) < radius", "2 * r");
        testRadiusExpressionInDistanceQuery("2 * r > ST_Distance(a, b)", "ST_Distance(a, b) < radius", "2 * r");
        testRadiusExpressionInDistanceQuery("2 * r > ST_Distance(b, a)", "ST_Distance(b, a) < radius", "2 * r");

        testRadiusExpressionInDistanceQuery("ST_Distance(a, b) <= 2 * r AND name_a != name_b", "ST_Distance(a, b) <= radius AND name_a != name_b", "2 * r");
        testRadiusExpressionInDistanceQuery("2 * r > ST_Distance(a, b) AND name_a != name_b", "ST_Distance(a, b) < radius AND name_a != name_b", "2 * r");

        testPointExpressionsInDistanceQuery("ST_Distance(ST_Point(lng_a, lat_a), ST_Point(lng_b, lat_b)) <= 5", "ST_Distance(point_a, point_b) <= radius", "5");
        testPointExpressionsInDistanceQuery("ST_Distance(ST_Point(lng_b, lat_b), ST_Point(lng_a, lat_a)) <= 5", "ST_Distance(point_b, point_a) <= radius", "5");
        testPointExpressionsInDistanceQuery("5 >= ST_Distance(ST_Point(lng_a, lat_a), ST_Point(lng_b, lat_b))", "ST_Distance(point_a, point_b) <= radius", "5");
        testPointExpressionsInDistanceQuery("5 >= ST_Distance(ST_Point(lng_b, lat_b), ST_Point(lng_a, lat_a))", "ST_Distance(point_b, point_a) <= radius", "5");

        testPointExpressionsInDistanceQuery("ST_Distance(ST_Point(lng_a, lat_a), ST_Point(lng_b, lat_b)) < 5", "ST_Distance(point_a, point_b) < radius", "5");
        testPointExpressionsInDistanceQuery("ST_Distance(ST_Point(lng_b, lat_b), ST_Point(lng_a, lat_a)) < 5", "ST_Distance(point_b, point_a) < radius", "5");
        testPointExpressionsInDistanceQuery("5 > ST_Distance(ST_Point(lng_a, lat_a), ST_Point(lng_b, lat_b))", "ST_Distance(point_a, point_b) < radius", "5");
        testPointExpressionsInDistanceQuery("5 > ST_Distance(ST_Point(lng_b, lat_b), ST_Point(lng_a, lat_a))", "ST_Distance(point_b, point_a) < radius", "5");

        testPointExpressionsInDistanceQuery("ST_Distance(ST_Point(lng_a, lat_a), ST_Point(lng_b, lat_b)) <= 5 AND name_a != name_b", "ST_Distance(point_a, point_b) <= radius AND name_a != name_b", "5");
        testPointExpressionsInDistanceQuery("5 > ST_Distance(ST_Point(lng_a, lat_a), ST_Point(lng_b, lat_b)) AND name_a != name_b", "ST_Distance(point_a, point_b) < radius AND name_a != name_b", "5");

        testPointAndRadiusExpressionsInDistanceQuery("ST_Distance(ST_Point(lng_a, lat_a), ST_Point(lng_b, lat_b)) <= 500 / (111000 * cos(lat_b))", "ST_Distance(point_a, point_b) <= radius", "500 / (111000 * cos(lat_b))");
        testPointAndRadiusExpressionsInDistanceQuery("ST_Distance(ST_Point(lng_b, lat_b), ST_Point(lng_a, lat_a)) <= 500 / (111000 * cos(lat_b))", "ST_Distance(point_b, point_a) <= radius", "500 / (111000 * cos(lat_b))");
        testPointAndRadiusExpressionsInDistanceQuery("500 / (111000 * cos(lat_b)) >= ST_Distance(ST_Point(lng_a, lat_a), ST_Point(lng_b, lat_b))", "ST_Distance(point_a, point_b) <= radius", "500 / (111000 * cos(lat_b))");
        testPointAndRadiusExpressionsInDistanceQuery("500 / (111000 * cos(lat_b)) >= ST_Distance(ST_Point(lng_b, lat_b), ST_Point(lng_a, lat_a))", "ST_Distance(point_b, point_a) <= radius", "500 / (111000 * cos(lat_b))");

        testPointAndRadiusExpressionsInDistanceQuery("ST_Distance(ST_Point(lng_a, lat_a), ST_Point(lng_b, lat_b)) < 500 / (111000 * cos(lat_b))", "ST_Distance(point_a, point_b) < radius", "500 / (111000 * cos(lat_b))");
        testPointAndRadiusExpressionsInDistanceQuery("ST_Distance(ST_Point(lng_b, lat_b), ST_Point(lng_a, lat_a)) < 500 / (111000 * cos(lat_b))", "ST_Distance(point_b, point_a) < radius", "500 / (111000 * cos(lat_b))");
        testPointAndRadiusExpressionsInDistanceQuery("500 / (111000 * cos(lat_b)) > ST_Distance(ST_Point(lng_a, lat_a), ST_Point(lng_b, lat_b))", "ST_Distance(point_a, point_b) < radius", "500 / (111000 * cos(lat_b))");
        testPointAndRadiusExpressionsInDistanceQuery("500 / (111000 * cos(lat_b)) > ST_Distance(ST_Point(lng_b, lat_b), ST_Point(lng_a, lat_a))", "ST_Distance(point_b, point_a) < radius", "500 / (111000 * cos(lat_b))");

        testPointAndRadiusExpressionsInDistanceQuery("ST_Distance(ST_Point(lng_a, lat_a), ST_Point(lng_b, lat_b)) <= 500 / (111000 * cos(lat_b)) AND name_a != name_b", "ST_Distance(point_a, point_b) <= radius AND name_a != name_b", "500 / (111000 * cos(lat_b))");
        testPointAndRadiusExpressionsInDistanceQuery("500 / (111000 * cos(lat_b)) > ST_Distance(ST_Point(lng_a, lat_a), ST_Point(lng_b, lat_b)) AND name_a != name_b", "ST_Distance(point_a, point_b) < radius AND name_a != name_b", "500 / (111000 * cos(lat_b))");
    }

    private void testSimpleDistanceQuery(String filter, String newFilter)
    {
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression(filter),
                                p.join(INNER,
                                        p.values(p.symbol("a"), p.symbol("name_a")),
                                        p.values(p.symbol("b"), p.symbol("name_b"), p.symbol("r")))))
                .matches(
                        spatialJoin(newFilter,
                                values(ImmutableMap.of("a", 0, "name_a", 1)),
                                values(ImmutableMap.of("b", 0, "name_b", 1, "r", 2))));
    }

    private void testRadiusExpressionInDistanceQuery(String filter, String newFilter, String radiusExpression)
    {
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression(filter),
                                p.join(INNER,
                                        p.values(p.symbol("a"), p.symbol("name_a")),
                                        p.values(p.symbol("b"), p.symbol("name_b"), p.symbol("r")))))
                .matches(
                        spatialJoin(newFilter,
                                values(ImmutableMap.of("a", 0, "name_a", 1)),
                                project(ImmutableMap.of("radius", expression(radiusExpression)),
                                        values(ImmutableMap.of("b", 0, "name_b", 1, "r", 2)))));
    }

    private void testPointExpressionsInDistanceQuery(String filter, String newFilter, String radiusExpression)
    {
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression(filter),
                                p.join(INNER,
                                        p.values(p.symbol("lat_a"), p.symbol("lng_a"), p.symbol("name_a")),
                                        p.values(p.symbol("lat_b"), p.symbol("lng_b"), p.symbol("name_b")))))
                .matches(
                        spatialJoin(newFilter,
                                project(ImmutableMap.of("point_a", expression("ST_Point(lng_a, lat_a)")),
                                        values(ImmutableMap.of("lat_a", 0, "lng_a", 1, "name_a", 2))),
                                project(ImmutableMap.of("point_b", expression("ST_Point(lng_b, lat_b)")),
                                        project(ImmutableMap.of("radius", expression(radiusExpression)), values(ImmutableMap.of("lat_b", 0, "lng_b", 1, "name_b", 2))))));
    }

    private void testPointAndRadiusExpressionsInDistanceQuery(String filter, String newFilter, String radiusExpression)
    {
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression(filter),
                                p.join(INNER,
                                        p.values(p.symbol("lat_a"), p.symbol("lng_a"), p.symbol("name_a")),
                                        p.values(p.symbol("lat_b"), p.symbol("lng_b"), p.symbol("name_b")))))
                .matches(
                        spatialJoin(newFilter,
                                project(ImmutableMap.of("point_a", expression("ST_Point(lng_a, lat_a)")),
                                        values(ImmutableMap.of("lat_a", 0, "lng_a", 1, "name_a", 2))),
                                project(ImmutableMap.of("point_b", expression("ST_Point(lng_b, lat_b)")),
                                        project(ImmutableMap.of("radius", expression(radiusExpression)),
                                                values(ImmutableMap.of("lat_b", 0, "lng_b", 1, "name_b", 2))))));
    }

    @Test
    public void testConvertToSpatialJoin()
    {
        // symbols
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("ST_Contains(a, b)"),
                                p.join(INNER,
                                        p.values(p.symbol("a")),
                                        p.values(p.symbol("b")))))
                .matches(
                        spatialJoin("ST_Contains(a, b)",
                                values(ImmutableMap.of("a", 0)),
                                values(ImmutableMap.of("b", 0))));

        // AND
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("name_1 != name_2 AND ST_Contains(a, b)"),
                                p.join(INNER,
                                        p.values(p.symbol("a"), p.symbol("name_1")),
                                        p.values(p.symbol("b"), p.symbol("name_2")))))
                .matches(
                        spatialJoin("name_1 != name_2 AND ST_Contains(a, b)",
                                values(ImmutableMap.of("a", 0, "name_1", 1)),
                                values(ImmutableMap.of("b", 0, "name_2", 1))));

        // AND
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("ST_Contains(a1, b1) AND ST_Contains(a2, b2)"),
                                p.join(INNER,
                                        p.values(p.symbol("a1"), p.symbol("a2")),
                                        p.values(p.symbol("b1"), p.symbol("b2")))))
                .matches(
                        spatialJoin("ST_Contains(a1, b1) AND ST_Contains(a2, b2)",
                                values(ImmutableMap.of("a1", 0, "a2", 1)),
                                values(ImmutableMap.of("b1", 0, "b2", 1))));
    }

    @Test
    public void testPushDownFirstArgument()
    {
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("ST_Contains(ST_GeometryFromText(wkt), point)"),
                                p.join(INNER,
                                        p.values(p.symbol("wkt", VARCHAR)),
                                        p.values(p.symbol("point", GEOMETRY)))))
                .matches(
                        spatialJoin("ST_Contains(st_geometryfromtext, point)",
                                project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0))),
                                values(ImmutableMap.of("point", 0))));

        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("ST_Contains(ST_GeometryFromText(wkt), ST_Point(0, 0))"),
                                p.join(INNER,
                                        p.values(p.symbol("wkt", VARCHAR)),
                                        p.values())))
                .doesNotFire();
    }

    @Test
    public void testPushDownSecondArgument()
    {
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("ST_Contains(polygon, ST_Point(lng, lat))"),
                                p.join(INNER,
                                        p.values(p.symbol("polygon", GEOMETRY)),
                                        p.values(p.symbol("lat"), p.symbol("lng")))))
                .matches(
                        spatialJoin("ST_Contains(polygon, st_point)",
                                values(ImmutableMap.of("polygon", 0)),
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1)))));

        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("ST_Contains(ST_GeometryFromText('POLYGON ...'), ST_Point(lng, lat))"),
                                p.join(INNER,
                                        p.values(),
                                        p.values(p.symbol("lat"), p.symbol("lng")))))
                .doesNotFire();
    }

    @Test
    public void testPushDownBothArguments()
    {
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))"),
                                p.join(INNER,
                                        p.values(p.symbol("wkt", VARCHAR)),
                                        p.values(p.symbol("lat"), p.symbol("lng")))))
                .matches(
                        spatialJoin("ST_Contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0))),
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1)))));
    }

    @Test
    public void testPushDownOppositeOrder()
    {
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))"),
                                p.join(INNER,
                                        p.values(p.symbol("lat"), p.symbol("lng")),
                                        p.values(p.symbol("wkt", VARCHAR)))))
                .matches(
                        spatialJoin("ST_Contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1))),
                                project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0)))));
    }

    @Test
    public void testPushDownAnd()
    {
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("name_1 != name_2 AND ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))"),
                                p.join(INNER,
                                        p.values(p.symbol("wkt", VARCHAR), p.symbol("name_1")),
                                        p.values(p.symbol("lat"), p.symbol("lng"), p.symbol("name_2")))))
                .matches(
                        spatialJoin("name_1 != name_2 AND ST_Contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0, "name_1", 1))),
                                project(ImmutableMap.of("st_point", expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1, "name_2", 2)))));

        // Multiple spatial functions - only the first one is being processed
        assertRuleApplication()
                .on(p ->
                        p.filter(PlanBuilder.expression("ST_Contains(ST_GeometryFromText(wkt1), geometry1) AND ST_Contains(ST_GeometryFromText(wkt2), geometry2)"),
                                p.join(INNER,
                                        p.values(p.symbol("wkt1", VARCHAR), p.symbol("wkt2", VARCHAR)),
                                        p.values(p.symbol("geometry1"), p.symbol("geometry2")))))
                .matches(
                        spatialJoin("ST_Contains(st_geometryfromtext, geometry1) AND ST_Contains(ST_GeometryFromText(wkt2), geometry2)",
                                project(ImmutableMap.of("st_geometryfromtext", expression("ST_GeometryFromText(wkt1)")), values(ImmutableMap.of("wkt1", 0, "wkt2", 1))),
                                values(ImmutableMap.of("geometry1", 0, "geometry2", 1))));
    }

    private RuleAssert assertRuleApplication()
    {
        return tester().assertThat(new ExtractSpatialInnerJoin(tester().getMetadata(), tester().getSplitManager(), tester().getPageSourceManager()));
    }
}
