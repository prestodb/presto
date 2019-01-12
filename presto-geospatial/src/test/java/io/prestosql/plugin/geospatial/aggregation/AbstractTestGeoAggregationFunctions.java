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
package io.prestosql.plugin.geospatial.aggregation;

import com.esri.core.geometry.ogc.OGCGeometry;
import io.airlift.slice.Slice;
import io.prestosql.block.BlockAssertions;
import io.prestosql.geospatial.serde.GeometrySerde;
import io.prestosql.metadata.FunctionKind;
import io.prestosql.metadata.Signature;
import io.prestosql.operator.aggregation.InternalAggregationFunction;
import io.prestosql.operator.scalar.AbstractTestFunctions;
import io.prestosql.plugin.geospatial.GeoPlugin;
import io.prestosql.plugin.geospatial.GeometryType;
import io.prestosql.spi.Page;
import io.prestosql.spi.type.Type;
import org.testng.annotations.BeforeClass;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;
import java.util.function.BiFunction;
import java.util.stream.Collectors;

import static io.prestosql.metadata.FunctionExtractor.extractFunctions;
import static io.prestosql.operator.aggregation.AggregationTestUtils.assertAggregation;
import static io.prestosql.spi.type.TypeSignature.parseTypeSignature;

public abstract class AbstractTestGeoAggregationFunctions
        extends AbstractTestFunctions
{
    private InternalAggregationFunction function;

    @BeforeClass
    public void registerFunctions()
    {
        GeoPlugin plugin = new GeoPlugin();
        for (Type type : plugin.getTypes()) {
            functionAssertions.getTypeRegistry().addType(type);
        }
        functionAssertions.getMetadata().addFunctions(extractFunctions(plugin.getFunctions()));
        function = functionAssertions
                .getMetadata()
                .getFunctionRegistry()
                .getAggregateFunctionImplementation(new Signature(
                        getFunctionName(),
                        FunctionKind.AGGREGATE,
                        parseTypeSignature(GeometryType.GEOMETRY_TYPE_NAME),
                        parseTypeSignature(GeometryType.GEOMETRY_TYPE_NAME)));
    }

    protected void assertAggregatedGeometries(String testDescription, String expectedWkt, String... wkts)
    {
        List<Slice> geometrySlices = Arrays.stream(wkts)
                .map(text -> text == null ? null : OGCGeometry.fromText(text))
                .map(input -> input == null ? null : GeometrySerde.serialize(input))
                .collect(Collectors.toList());

        // Add a custom equality assertion because the resulting geometry may have
        // its constituent points in a different order
        BiFunction<Object, Object, Boolean> equalityFunction = (left, right) -> {
            if (left == null && right == null) {
                return true;
            }
            if (left == null || right == null) {
                return false;
            }
            OGCGeometry leftGeometry = OGCGeometry.fromText(left.toString());
            OGCGeometry rightGeometry = OGCGeometry.fromText(right.toString());
            // Check for equality by getting the difference
            return leftGeometry.difference(rightGeometry).isEmpty() &&
                    rightGeometry.difference(leftGeometry).isEmpty();
        };
        // Test in forward and reverse order to verify that ordering doesn't affect the output
        assertAggregation(function, equalityFunction, testDescription,
                new Page(BlockAssertions.createSlicesBlock(geometrySlices)), expectedWkt);
        Collections.reverse(geometrySlices);
        assertAggregation(function, equalityFunction, testDescription,
                new Page(BlockAssertions.createSlicesBlock(geometrySlices)), expectedWkt);
    }

    protected abstract String getFunctionName();
}
