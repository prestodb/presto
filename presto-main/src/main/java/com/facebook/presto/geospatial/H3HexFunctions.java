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
package com.facebook.presto.geospatial;

import com.facebook.presto.common.block.ArrayBlockBuilder;
import com.facebook.presto.common.block.Block;
import com.facebook.presto.common.block.BlockBuilder;
import com.facebook.presto.common.type.StandardTypes;
import com.facebook.presto.spi.PrestoException;
import com.facebook.presto.spi.function.Description;
import com.facebook.presto.spi.function.ScalarFunction;
import com.facebook.presto.spi.function.SqlNullable;
import com.facebook.presto.spi.function.SqlType;
import com.uber.h3core.AreaUnit;
import com.uber.h3core.H3CoreV3;
import com.uber.h3core.LengthUnit;
import com.uber.h3core.util.LatLng;
import io.airlift.slice.Slice;
import io.airlift.slice.Slices;

import java.io.IOException;
import java.util.ArrayList;
import java.util.List;
import java.util.stream.Collectors;

import static com.facebook.presto.common.type.DoubleType.DOUBLE;
import static com.facebook.presto.common.type.VarcharType.VARCHAR;
import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;

public final class H3HexFunctions
{
    private static final H3CoreV3 h3;

    private H3HexFunctions() {}

    static {
        try {
            h3 = H3CoreV3.newInstance();
        }
        catch (IOException ex) {
            throw new RuntimeException("Failed to load H3Core", ex);
        }
    }

    private static Block convertHexagonAddrArrayToBlock(List<String> hexAddrs)
    {
        BlockBuilder block = VARCHAR.createBlockBuilder(null, hexAddrs.size());
        for (String hexAddr : hexAddrs) {
            VARCHAR.writeString(block, hexAddr);
        }
        return block.build();
    }

    private static List<String> convertBlockToHexagonAddrs(Block hexAddrBlock)
    {
        List<String> hexAddrsStr = new ArrayList<>();
        for (int i = 0; i < hexAddrBlock.getPositionCount(); i++) {
            Slice hexAddr = VARCHAR.getSlice(hexAddrBlock, i);
            if (hexAddr.length() == 0) {
                throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Input array contains an empty value.");
            }
            String hexAddrStr = unwrapAddress(hexAddr);
            hexAddrsStr.add(hexAddrStr);
        }
        return hexAddrsStr;
    }

    private static AreaUnit getAreaUnit(String unitStr)
            throws PrestoException
    {
        if (unitStr.equals("km2")) {
            return AreaUnit.km2;
        }
        else if (unitStr.equals("m2")) {
            return AreaUnit.m2;
        }
        else {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Area unit must be 'm2' or 'km2'.");
        }
    }

    private static String unwrapAddress(@SqlType(StandardTypes.VARCHAR) Slice hexAddr)
            throws PrestoException
    {
        String hexAddress = hexAddr.toStringUtf8();
        if (!h3.h3IsValid(hexAddress)) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Input is not a valid h3 address (" + hexAddress + ").");
        }
        return hexAddress;
    }

    private static LengthUnit getLengthUnit(String unitStr)
            throws PrestoException
    {
        if (unitStr.equals("km")) {
            return LengthUnit.km;
        }
        else if (unitStr.equals("m")) {
            return LengthUnit.m;
        }
        else {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Length unit must be 'm' or 'km'.");
        }
    }

    @Description("Returns hexagon address calculated from H3 library.\\nlat(double): latitude of the coordinate\\nlng(double): longitude of the coordinate\\nres(int): resolution of the address, between 0 and 15 inclusive")
    @ScalarFunction("get_hexagon_addr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getHexagonAddr(@SqlType(StandardTypes.DOUBLE) double lat,
            @SqlType(StandardTypes.DOUBLE) double lng,
            @SqlType(StandardTypes.BIGINT) long res)
    {
        return Slices.utf8Slice(h3.geoToH3Address(lat, lng, (int) res));
    }

    @SqlNullable
    @Description("Returns wkt from hex address from H3 library.")
    @ScalarFunction("get_hexagon_addr_wkt")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getHexagonAddrWkt(@SqlType(StandardTypes.VARCHAR) Slice hexAddr)
    {
        if (hexAddr.length() == 0) {
            return null;
        }

        String hexAddress = unwrapAddress(hexAddr);
        List<LatLng> boundaries = h3.h3ToGeoBoundary(hexAddress);

        //Polygons have a minimum of 3 points
        if (boundaries.size() < 3) {
            return null;
        }

        //We have to check that the points make a valid polygon according to the WKT OGC standard
        // https://portal.ogc.org/files/?artifact_id=25355
        if (!boundaries.get(0).equals(boundaries.get(boundaries.size() - 1))) {
            boundaries.add(boundaries.get(0));
        }

        //Convert to a List of Strings
        String result = boundaries.stream().map(geoCoord -> geoCoord.lng + " " + geoCoord.lat).collect(Collectors.joining(","));
        return Slices.utf8Slice("POLYGON ((" + result + "))");
    }

    @Description("Return the parent hexagon address of the input hexagon.\\n  hexAddr(string): child hexagon address\\n  res(int): target parent resolution, must not be greater than the resolution of the input hexagon")
    @ScalarFunction("get_parent_hexagon_addr")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice getParentHexagonAddr(@SqlType(StandardTypes.VARCHAR) Slice hexAddr,
            @SqlType(StandardTypes.BIGINT) long res)
    {
        return Slices.utf8Slice(h3.h3ToParentAddress(hexAddr.toStringUtf8(), (int) res));
    }

    @SqlNullable
    @Description("Return the hex distance between index1 and index2.\\n  hexAddr1(string): first hexagon address\\n  hexAddr2(string): second hexagon address")
    @ScalarFunction("h3_distance")
    @SqlType(StandardTypes.BIGINT)
    public static Long getHexDistance(@SqlType(StandardTypes.VARCHAR) Slice hexAddr1,
            @SqlType(StandardTypes.VARCHAR) Slice hexAddr2)
    {
        if (hexAddr1.length() == 0 || hexAddr2.length() == 0) {
            return null;
        }

        String hexAddrStr1 = unwrapAddress(hexAddr1);
        String hexAddrStr2 = unwrapAddress(hexAddr2);
        return (long) h3.h3Distance(hexAddrStr1, hexAddrStr2);
    }

    @SqlNullable
    @Description("Return the centroid coordinates (degrees latitude, degrees longitude) of hexagon address.\\n  hexAddr(string): hexagon address")
    @ScalarFunction("h3_to_geo")
    @SqlType("array(double)")
    public static Block getGeometryFromH3Address(@SqlType(StandardTypes.VARCHAR) Slice hexAddr)
    {
        if (hexAddr.length() == 0) {
            return null;
        }

        String hexAddrStr = unwrapAddress(hexAddr);
        LatLng centroid = h3.h3ToGeo(hexAddrStr);
        BlockBuilder blockBuilder = DOUBLE.createBlockBuilder(null, 2);
        DOUBLE.writeDouble(blockBuilder, centroid.lat);
        DOUBLE.writeDouble(blockBuilder, centroid.lng);
        return blockBuilder.build();
    }

    @SqlNullable
    @Description("Return the edge length of a hexagon at a given resolution level in m or km.\\n  resolutionLevel(int): H3 resolution level\\n  unit(string): unit of length")
    @ScalarFunction("h3_edge_length")
    @SqlType(StandardTypes.DOUBLE)
    public static Double getH3EdgeLength(@SqlType(StandardTypes.BIGINT) long resolutionLevel,
            @SqlType(StandardTypes.VARCHAR) Slice unit)
    {
        if (unit.length() == 0) {
            return null;
        }
        String unitStr = unit.toStringUtf8();
        return h3.edgeLength((int) resolutionLevel, getLengthUnit(unitStr));
    }

    @SqlNullable
    @Description("Return the area of hexagon at a given resolution level in 'm2' or 'km2'.\\n  resolutionLevel(int): H3 resolution level\\n  unit(string): unit of area")
    @ScalarFunction("h3_hex_area")
    @SqlType(StandardTypes.DOUBLE)
    public static Double getH3HexArea(@SqlType(StandardTypes.BIGINT) long resolutionLevel,
            @SqlType(StandardTypes.VARCHAR) Slice unit)
    {
        if (unit.length() == 0) {
            return null;
        }
        String unitStr = unit.toStringUtf8();
        return h3.hexArea((int) resolutionLevel, getAreaUnit(unitStr));
    }

    @SqlNullable
    @Description("Return resolution level of hexagon addresss.\\n  hexAddr(string): hexagon address")
    @ScalarFunction("h3_resolution")
    @SqlType(StandardTypes.BIGINT)
    public static Long getH3Resolution(@SqlType(StandardTypes.VARCHAR) Slice hexAddr)
    {
        if (hexAddr.length() == 0) {
            return null;
        }
        String hexAddrStr = unwrapAddress(hexAddr);
        return (long) h3.h3GetResolution(hexAddrStr);
    }

    @SqlNullable
    @Description("Return child indices of a given hexagon address.\\n  hexAddr(string): hexagon address\\n  resolutionLevel(int): resolution of child indexes")
    @ScalarFunction("h3_to_children")
    @SqlType("array(varchar)")
    public static Block getH3ToChildren(@SqlType(StandardTypes.VARCHAR) Slice hexAddr,
            @SqlType(StandardTypes.BIGINT) long resolutionLevel)
    {
        if (hexAddr.length() == 0) {
            return null;
        }

        String hexAddrStr = unwrapAddress(hexAddr);
        List<String> children = h3.h3ToChildren(hexAddrStr, (int) resolutionLevel);
        return convertHexagonAddrArrayToBlock(children);
    }

    @SqlNullable
    @Description("Return list of rings of a list of addresses around an origin hexagon.\\n  hexAddr(string): origin hexagon address\\n  num_rings(int): number of rings")
    @ScalarFunction("h3_hex_range")
    @SqlType("array(array(varchar))")
    public static Block getHexRange(@SqlType(StandardTypes.VARCHAR) Slice hexAddr,
            @SqlType(StandardTypes.BIGINT) long numRings)
    {
        if (hexAddr.length() == 0) {
            return null;
        }

        if (numRings < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Number of rings must be greater than or equal to 0.");
        }

        String hexAddrStr = unwrapAddress(hexAddr);
        List<List<String>> hexRange = h3.hexRange(hexAddrStr, (int) numRings);

        BlockBuilder blockHex = new ArrayBlockBuilder(VARCHAR, null, hexRange.size());
        for (List<String> ring : hexRange) {
            Block block = convertHexagonAddrArrayToBlock(ring);
            blockHex.appendStructure(block);
        }
        return blockHex.build();
    }

    @SqlNullable
    @Description("Return hexagon addresses in ring number given a central hexagon.\\n  hexAddr(string): origin hexagon address\\n  ringNumber(int): hexagon ring number")
    @ScalarFunction("h3_hex_ring")
    @SqlType("array(varchar)")
    public static Block getHexRing(@SqlType(StandardTypes.VARCHAR) Slice hexAddr,
            @SqlType(StandardTypes.BIGINT) long ringNumber)
    {
        if (hexAddr.length() == 0) {
            return null;
        }

        if (ringNumber < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Ring number must be greater than or equal to 0.");
        }

        String hexAddrStr = unwrapAddress(hexAddr);
        List<String> hexRing = h3.hexRing(hexAddrStr, (int) ringNumber);
        return convertHexagonAddrArrayToBlock(hexRing);
    }

    @SqlNullable
    @Description("Return neighboring indexes around given hexagon address; does not fail around pentagons.\\n  hexAddr(string): origin hexagon address\\n  numRings(string): number of rings around the origin")
    @ScalarFunction("h3_kring")
    @SqlType("array(varchar)")
    public static Block getKRing(@SqlType(StandardTypes.VARCHAR) Slice hexAddr,
            @SqlType(StandardTypes.BIGINT) long numRings)
    {
        if (hexAddr.length() == 0) {
            return null;
        }

        if (numRings < 0) {
            throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Number of rings must be greater than or equal to 0.");
        }

        String hexAddrStr = unwrapAddress(hexAddr);
        List<String> kRing = h3.kRing(hexAddrStr, (int) numRings);
        return convertHexagonAddrArrayToBlock(kRing);
    }

    @SqlNullable
    @Description("Returns if a given hexagon address is a pentagon.\\n  hexAddr(string): hexagon address")
    @ScalarFunction("h3_is_pentagon")
    @SqlType(StandardTypes.BOOLEAN)
    public static Boolean isPentagon(@SqlType(StandardTypes.VARCHAR) Slice hexAddr)
    {
        if (hexAddr.length() == 0) {
            return null;
        }

        String hexAddrStr = unwrapAddress(hexAddr);
        return h3.h3IsPentagon(hexAddrStr);
    }

    @SqlNullable
    @Description("Returns an uncompacted set of indexes at a resolution level\\n  hexAddrs(array(string)): Array of h3 indexes\\n  resolution(int): Resolution level")
    @ScalarFunction("h3_uncompact")
    @SqlType("array(varchar)")
    public static Block getUncompact(@SqlType("array(varchar)") Block hexAddrs,
            @SqlType(StandardTypes.BIGINT) long resolution)
    {
        List<String> hexAddrsStr = convertBlockToHexagonAddrs(hexAddrs);
        List<String> uncompactAddresses = h3.uncompactAddress(hexAddrsStr, (int) resolution);
        return convertHexagonAddrArrayToBlock(uncompactAddresses);
    }

    @SqlNullable
    @Description("Returns a compacted set of indexes\\n hexAddrs(array(string)): Array of h3 indexes")
    @ScalarFunction("h3_compact")
    @SqlType("array(varchar)")
    public static Block getCompact(@SqlType("array(varchar)") Block hexAddrs)
    {
        List<String> hexAddrsStr = convertBlockToHexagonAddrs(hexAddrs);
        List<String> compactAddresses = h3.compactAddress(hexAddrsStr);
        return convertHexagonAddrArrayToBlock(compactAddresses);
    }

    @SqlNullable
    @Description("Returns a long data type'd hex address to its string counterpart")
    @ScalarFunction("h3_to_string")
    @SqlType(StandardTypes.VARCHAR)
    public static Slice h3ToString(@SqlType(StandardTypes.BIGINT) long longAddress)
    {
        return Slices.utf8Slice(h3.h3ToString(longAddress));
    }
}
