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
package io.prestosql.geospatial;

import io.airlift.json.JsonCodec;
import io.airlift.json.JsonCodecFactory;

import static java.util.Objects.requireNonNull;

public class KdbTreeUtils
{
    private static final JsonCodec<KdbTree> KDB_TREE_CODEC = new JsonCodecFactory().jsonCodec(KdbTree.class);

    private KdbTreeUtils() {}

    public static KdbTree fromJson(String json)
    {
        requireNonNull(json, "json is null");
        return KDB_TREE_CODEC.fromJson(json);
    }

    public static String toJson(KdbTree kdbTree)
    {
        requireNonNull(kdbTree, "kdbTree is null");
        return KDB_TREE_CODEC.toJson(kdbTree);
    }
}
