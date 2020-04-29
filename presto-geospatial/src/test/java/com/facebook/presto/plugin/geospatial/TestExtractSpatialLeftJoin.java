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

import com.facebook.presto.common.type.Type;
import com.facebook.presto.spi.relation.RowExpression;
import com.facebook.presto.sql.TestingRowExpressionTranslator;
import com.facebook.presto.sql.planner.TypeProvider;
import com.facebook.presto.sql.planner.assertions.PlanMatchPattern;
import com.facebook.presto.sql.planner.iterative.rule.ExtractSpatialJoins.ExtractSpatialLeftJoin;
import com.facebook.presto.sql.planner.iterative.rule.test.BaseRuleTest;
import com.facebook.presto.sql.planner.iterative.rule.test.PlanBuilder;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleAssert;
import com.facebook.presto.sql.planner.iterative.rule.test.RuleTester;
import com.google.common.collect.ImmutableMap;
import org.testng.annotations.BeforeClass;
import org.testng.annotations.Test;

import java.util.Map;

import static com.facebook.presto.common.type.BigintType.BIGINT;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.plugin.geospatial.GeometryType.GEOMETRY;
import static com.facebook.presto.plugin.geospatial.SphericalGeographyType.SPHERICAL_GEOGRAPHY;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.project;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.spatialLeftJoin;
import static com.facebook.presto.sql.planner.assertions.PlanMatchPattern.values;
import static com.facebook.presto.sql.planner.plan.JoinNode.Type.LEFT;

public class TestExtractSpatialLeftJoin
        extends BaseRuleTest
{
    private TestingRowExpressionTranslator sqlToRowExpressionTranslator;

    public TestExtractSpatialLeftJoin()
    {
        super(new GeoPlugin());
    }

    @BeforeClass
    public void setupTranslator()
    {
        this.sqlToRowExpressionTranslator = new TestingRowExpressionTranslator(tester().getMetadata());
    }

    @Test
    public void testDoesNotFire()
    {
        // scalar expression
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(),
                                p.values(p.variable("b", GEOMETRY)),
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText('POLYGON ((0 0, 0 0, 0 0, 0 0))'), b)",
                                        ImmutableMap.of("b", GEOMETRY))))
                .doesNotFire();

        // OR operand
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt", VARCHAR), p.variable("name_1")),
                                p.values(p.variable("point", GEOMETRY), p.variable("name_2")),
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText(wkt), point) OR name_1 != name_2",
                                        ImmutableMap.of("wkt", VARCHAR, "point", GEOMETRY, "name_1", BIGINT, "name_2", BIGINT))))
                .doesNotFire();

        // NOT operator
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt", VARCHAR), p.variable("name_1")),
                                p.values(p.variable("point", GEOMETRY), p.variable("name_2")),
                                sqlToRowExpression(
                                        "NOT ST_Contains(ST_GeometryFromText(wkt), point)",
                                        ImmutableMap.of("wkt", VARCHAR, "point", GEOMETRY, "name_1", BIGINT, "name_2", BIGINT))))
                .doesNotFire();

        // ST_Distance(...) > r
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("a", GEOMETRY)),
                                p.values(p.variable("b", GEOMETRY)),
                                sqlToRowExpression(
                                        "ST_Distance(a, b) > 5",
                                        ImmutableMap.of("a", GEOMETRY, "b", GEOMETRY))))
                .doesNotFire();

        // SphericalGeography operand
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("a", SPHERICAL_GEOGRAPHY)),
                                p.values(p.variable("b", SPHERICAL_GEOGRAPHY)),
                                sqlToRowExpression(
                                        "ST_Distance(a, b) < 5",
                                        ImmutableMap.of("a", SPHERICAL_GEOGRAPHY, "b", SPHERICAL_GEOGRAPHY))))
                .doesNotFire();

        // to_spherical_geography() operand
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt", VARCHAR)),
                                p.values(p.variable("point", SPHERICAL_GEOGRAPHY)),
                                sqlToRowExpression(
                                        "ST_Distance(to_spherical_geography(ST_GeometryFromText(wkt)), point) < 5",
                                        ImmutableMap.of("wkt", VARCHAR, "point", SPHERICAL_GEOGRAPHY))))
                .doesNotFire();
    }

    @Test(enabled = false)
    public void testSphericalGeographiesDoesNotFire()
    {
        // TODO enable once #13133 is merged
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("polygon", SPHERICAL_GEOGRAPHY)),
                                p.values(p.variable("point", SPHERICAL_GEOGRAPHY)),
                                sqlToRowExpression(
                                        "ST_Contains(polygon, point)",
                                        ImmutableMap.of("polygon", SPHERICAL_GEOGRAPHY, "point", SPHERICAL_GEOGRAPHY))))
                .doesNotFire();

        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt", VARCHAR)),
                                p.values(p.variable("point", SPHERICAL_GEOGRAPHY)),
                                sqlToRowExpression(
                                        "ST_Contains(to_spherical_geography(ST_GeometryFromText(wkt)), point)",
                                        ImmutableMap.of("wkt", VARCHAR, "point", SPHERICAL_GEOGRAPHY))))
                .doesNotFire();
    }

    @Test
    public void testConvertToSpatialJoin()
    {
        // symbols
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("a", GEOMETRY)),
                                p.values(p.variable("b", GEOMETRY)),
                                sqlToRowExpression("ST_Contains(a, b)", ImmutableMap.of("a", GEOMETRY, "b", GEOMETRY))))
                .matches(
                        spatialLeftJoin("ST_Contains(a, b)",
                                values(ImmutableMap.of("a", 0)),
                                values(ImmutableMap.of("b", 0))));

        // AND
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("a", GEOMETRY), p.variable("name_1")),
                                p.values(p.variable("b", GEOMETRY), p.variable("name_2")),
                                sqlToRowExpression("name_1 != name_2 AND ST_Contains(a, b)", ImmutableMap.of("a", GEOMETRY, "b", GEOMETRY, "name_1", BIGINT, "name_2", BIGINT))))
                .matches(
                        spatialLeftJoin("name_1 != name_2 AND ST_Contains(a, b)",
                                values(ImmutableMap.of("a", 0, "name_1", 1)),
                                values(ImmutableMap.of("b", 0, "name_2", 1))));

        // AND
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("a1", GEOMETRY), p.variable("a2", GEOMETRY)),
                                p.values(p.variable("b1", GEOMETRY), p.variable("b2", GEOMETRY)),
                                sqlToRowExpression("ST_Contains(a1, b1) AND ST_Contains(a2, b2)", ImmutableMap.of("a1", GEOMETRY, "b1", GEOMETRY, "a2", GEOMETRY, "b2", GEOMETRY))))
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
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText(wkt), point)",
                                        ImmutableMap.of("wkt", VARCHAR, "point", GEOMETRY))))
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, point)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0))),
                                values(ImmutableMap.of("point", 0))));

        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt", VARCHAR)),
                                p.values(),
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText(wkt), ST_Point(0, 0))",
                                        ImmutableMap.of("wkt", VARCHAR))))
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
                                sqlToRowExpression(
                                        "ST_Contains(polygon, ST_Point(lng, lat))",
                                        ImmutableMap.of("polygon", GEOMETRY, "lat", BIGINT, "lng", BIGINT))))
                .matches(
                        spatialLeftJoin("ST_Contains(polygon, st_point)",
                                values(ImmutableMap.of("polygon", 0)),
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1)))));

        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(),
                                p.values(p.variable("lat"), p.variable("lng")),
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText('POLYGON ((0 0, 0 0, 0 0, 0 0))'), ST_Point(lng, lat))",
                                        ImmutableMap.of("polygon", GEOMETRY, "lat", BIGINT, "lng", BIGINT))))
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
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                                        ImmutableMap.of("wkt", VARCHAR, "lat", BIGINT, "lng", BIGINT))))
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
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                                        ImmutableMap.of("wkt", VARCHAR, "lat", BIGINT, "lng", BIGINT))))
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
                                sqlToRowExpression(
                                        "name_1 != name_2 AND ST_Contains(ST_GeometryFromText(wkt), ST_Point(lng, lat))",
                                        ImmutableMap.of("wkt", VARCHAR, "name_1", BIGINT, "name_2", BIGINT, "lat", BIGINT, "lng", BIGINT))))
                .matches(
                        spatialLeftJoin("name_1 != name_2 AND ST_Contains(st_geometryfromtext, st_point)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt)")), values(ImmutableMap.of("wkt", 0, "name_1", 1))),
                                project(ImmutableMap.of("st_point", PlanMatchPattern.expression("ST_Point(lng, lat)")), values(ImmutableMap.of("lat", 0, "lng", 1, "name_2", 2)))));

        // Multiple spatial functions - only the first one is being processed
        assertRuleApplication()
                .on(p ->
                        p.join(LEFT,
                                p.values(p.variable("wkt1", VARCHAR), p.variable("wkt2", VARCHAR)),
                                p.values(p.variable("geometry1", GEOMETRY), p.variable("geometry2", GEOMETRY)),
                                sqlToRowExpression(
                                        "ST_Contains(ST_GeometryFromText(wkt1), geometry1) AND ST_Contains(ST_GeometryFromText(wkt2), geometry2)",
                                        ImmutableMap.of("wkt1", VARCHAR, "wkt2", VARCHAR, "geometry1", GEOMETRY, "geometry2", GEOMETRY))))
                .matches(
                        spatialLeftJoin("ST_Contains(st_geometryfromtext, geometry1) AND ST_Contains(ST_GeometryFromText(wkt2), geometry2)",
                                project(ImmutableMap.of("st_geometryfromtext", PlanMatchPattern.expression("ST_GeometryFromText(wkt1)")), values(ImmutableMap.of("wkt1", 0, "wkt2", 1))),
                                values(ImmutableMap.of("geometry1", 0, "geometry2", 1))));
    }

    private RuleAssert assertRuleApplication()
    {
        RuleTester tester = tester();
        return tester().assertThat(new ExtractSpatialLeftJoin(tester.getMetadata(), tester.getSplitManager(), tester.getPageSourceManager()));
    }

    private RowExpression sqlToRowExpression(String sql, Map<String, Type> typeMap)
    {
        return sqlToRowExpressionTranslator.translateAndOptimize(PlanBuilder.expression(sql), TypeProvider.copyOf(typeMap));
    }
}
