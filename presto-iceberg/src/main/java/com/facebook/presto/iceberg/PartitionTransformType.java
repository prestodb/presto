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
package com.facebook.presto.iceberg;

import java.util.HashMap;
import java.util.Map;
import java.util.Optional;

public enum PartitionTransformType
{
    IDENTITY("identity", 0),
    YEAR("year", 1),
    MONTH("month", 2),
    DAY("day", 3),
    HOUR("hour", 4),
    BUCKET("bucket", 5),
    TRUNCATE("truncate", 6);

    private static final Map<String, PartitionTransformType> TRANSFORM_MAP = new HashMap<>();
    private static final Map<Integer, PartitionTransformType> CODE_MAP = new HashMap<>();

    static {
        for (PartitionTransformType type : values()) {
            TRANSFORM_MAP.put(type.transform, type);
            CODE_MAP.put(type.code, type);
        }
    }

    private final String transform;
    private final int code;

    PartitionTransformType(String transform, int code)
    {
        this.transform = transform;
        this.code = code;
    }

    public String getTransform()
    {
        return transform;
    }

    public int getCode()
    {
        return code;
    }

    public static Optional<PartitionTransformType> fromString(String transform)
    {
        if (transform == null) {
            return Optional.empty();
        }

        PartitionTransformType type = TRANSFORM_MAP.get(transform);
        if (type != null) {
            return Optional.of(type);
        }

        // Handle bucket and truncate transforms with parameters
        if (transform.startsWith(BUCKET.transform + "[")) {
            return Optional.of(BUCKET);
        }
        if (transform.startsWith(TRUNCATE.transform + "[")) {
            return Optional.of(TRUNCATE);
        }

        return Optional.empty();
    }

    public static PartitionTransformType fromCode(int code)
    {
        PartitionTransformType type = CODE_MAP.get(code);
        if (type == null) {
            throw new IllegalArgumentException("Unknown transform code: " + code);
        }
        return type;
    }

    public static PartitionTransformType fromStringOrFail(String transform)
    {
        return fromString(transform)
                .orElseThrow(() -> new IllegalArgumentException("Unsupported transform type: " + transform));
    }

    @Override
    public String toString()
    {
        return transform;
    }
}
