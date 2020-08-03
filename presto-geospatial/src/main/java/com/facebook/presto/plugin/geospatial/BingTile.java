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

import com.facebook.presto.spi.PrestoException;
import com.fasterxml.jackson.annotation.JsonCreator;
import com.fasterxml.jackson.annotation.JsonProperty;
import com.google.common.annotations.VisibleForTesting;
import com.google.common.collect.ImmutableList;

import java.util.List;
import java.util.Objects;

import static com.facebook.presto.spi.StandardErrorCode.INVALID_FUNCTION_ARGUMENT;
import static com.google.common.base.MoreObjects.toStringHelper;
import static com.google.common.base.Preconditions.checkArgument;
import static java.lang.String.format;

public final class BingTile
{
    public static final int MAX_ZOOM_LEVEL = 23;
    @VisibleForTesting
    static final int VERSION_OFFSET = 63 - 5;
    private static final int VERSION = 0;
    private static final int BITS_23 = (1 << 24) - 1;
    private static final int BITS_5 = (1 << 6) - 1;
    private static final int ZOOM_OFFSET = 31 - 5;

    private final int x;
    private final int y;
    private final int zoomLevel;

    private BingTile(int x, int y, int zoomLevel)
    {
        checkArgument(0 <= zoomLevel && zoomLevel <= MAX_ZOOM_LEVEL);
        checkArgument(0 <= x && x < (1 << zoomLevel));
        checkArgument(0 <= y && y < (1 << zoomLevel));
        this.x = x;
        this.y = y;
        this.zoomLevel = zoomLevel;
    }

    @Override
    public boolean equals(Object other)
    {
        if (this == other) {
            return true;
        }

        if (other == null || getClass() != other.getClass()) {
            return false;
        }

        BingTile otherTile = (BingTile) other;
        return this.x == otherTile.x &&
                this.y == otherTile.y &&
                this.zoomLevel == otherTile.zoomLevel;
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(x, y, zoomLevel);
    }

    @Override
    public String toString()
    {
        return toStringHelper(this)
                .add("x", x)
                .add("y", y)
                .add("zoom_level", zoomLevel)
                .toString();
    }

    @JsonCreator
    public static BingTile fromCoordinates(
            @JsonProperty("x") int x,
            @JsonProperty("y") int y,
            @JsonProperty("zoom") int zoomLevel)
    {
        return new BingTile(x, y, zoomLevel);
    }

    public static BingTile fromQuadKey(String quadKey)
    {
        int zoomLevel = quadKey.length();
        checkArgument(zoomLevel <= MAX_ZOOM_LEVEL);
        int tileX = 0;
        int tileY = 0;
        for (int i = zoomLevel; i > 0; i--) {
            int mask = 1 << (i - 1);
            switch (quadKey.charAt(zoomLevel - i)) {
                case '0':
                    break;
                case '1':
                    tileX |= mask;
                    break;
                case '2':
                    tileY |= mask;
                    break;
                case '3':
                    tileX |= mask;
                    tileY |= mask;
                    break;
                default:
                    throw new PrestoException(INVALID_FUNCTION_ARGUMENT, "Invalid QuadKey digit sequence: " + quadKey);
            }
        }

        return new BingTile(tileX, tileY, zoomLevel);
    }

    @JsonProperty
    public int getX()
    {
        return x;
    }

    @JsonProperty
    public int getY()
    {
        return y;
    }

    @JsonProperty("zoom")
    public int getZoomLevel()
    {
        return zoomLevel;
    }

    public String toQuadKey()
    {
        char[] quadKey = new char[this.zoomLevel];
        for (int i = this.zoomLevel; i > 0; i--) {
            char digit = '0';
            int mask = 1 << (i - 1);
            if ((this.x & mask) != 0) {
                digit++;
            }
            if ((this.y & mask) != 0) {
                digit += 2;
            }
            quadKey[this.zoomLevel - i] = digit;
        }
        return String.valueOf(quadKey);
    }

    public List<BingTile> findChildren()
    {
        return findChildren(zoomLevel + 1);
    }

    public List<BingTile> findChildren(int newZoom)
    {
        if (newZoom == zoomLevel) {
            return ImmutableList.of(this);
        }

        checkArgument(newZoom <= MAX_ZOOM_LEVEL, "newZoom must be less than or equal to %s: %s", MAX_ZOOM_LEVEL, newZoom);
        checkArgument(newZoom >= zoomLevel, "newZoom must be greater than or equal to current zoom %s: %s", zoomLevel, newZoom);

        int zoomDelta = newZoom - zoomLevel;
        int xNew = x << zoomDelta;
        int yNew = y << zoomDelta;
        ImmutableList.Builder<BingTile> builder = ImmutableList.builderWithExpectedSize(1 << (2 * zoomDelta));
        for (int yDelta = 0; yDelta < 1 << zoomDelta; ++yDelta) {
            for (int xDelta = 0; xDelta < 1 << zoomDelta; ++xDelta) {
                builder.add(BingTile.fromCoordinates(xNew + xDelta, yNew + yDelta, newZoom));
            }
        }
        return builder.build();
    }

    public BingTile findParent()
    {
        return findParent(zoomLevel - 1);
    }

    public BingTile findParent(int newZoom)
    {
        if (newZoom == zoomLevel) {
            return this;
        }

        checkArgument(newZoom >= 0, "newZoom must be greater than or equal to 0: %s", newZoom);
        checkArgument(newZoom <= zoomLevel, "newZoom must be less than or equal to current zoom %s: %s", zoomLevel, newZoom);

        int zoomDelta = zoomLevel - newZoom;
        return BingTile.fromCoordinates(x >> zoomDelta, y >> zoomDelta, newZoom);
    }

    /**
     * Encodes Bing tile as a 64-bit long:
     * Version (5 bits), 0 (4 bits), x (23 bits), Zoom (5 bits), 0 (4 bits), y (23 bits)
     * (high bits left, low bits right).
     * <p>
     * This arrangement maximizes low-bit entropy for the Java long hash function.
     */
    public long encode()
    {
        // Java's long hash function just XORs itself right shifted 32.
        // This is used for bucketing, so if you have 2^k buckets, this only
        // keeps the k lowest bits.  This puts the highest entropy bits
        // (finest resolution x and y bits) in places that contribute to the
        // low bits of the hash.
        return (((long) VERSION << VERSION_OFFSET) | y | ((long) x << 32) | ((long) zoomLevel << ZOOM_OFFSET));
    }

    public static BingTile decode(long tile)
    {
        int version = (int) (tile >>> VERSION_OFFSET) & BITS_5;
        if (version == 0) {
            return decodeV0(tile);
        }
        else {
            throw new IllegalArgumentException(format("Unknown Bing Tile encoding version: %s", version));
        }
    }

    private static BingTile decodeV0(long tile)
    {
        int tileX = (int) (tile >>> 32) & BITS_23;
        int tileY = (int) tile & BITS_23;
        int zoomLevel = (int) (tile >>> ZOOM_OFFSET) & BITS_5;

        return new BingTile(tileX, tileY, zoomLevel);
    }
}
