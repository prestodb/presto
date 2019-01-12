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
package io.prestosql.testing;

import io.prestosql.block.BlockEncodingManager;
import io.prestosql.metadata.FunctionRegistry;
import io.prestosql.spi.type.TypeManager;
import io.prestosql.sql.analyzer.FeaturesConfig;
import io.prestosql.type.TypeRegistry;

public class TestingEnvironment
{
    private TestingEnvironment() {}

    public static final TypeManager TYPE_MANAGER = new TypeRegistry();

    static {
        // wire TYPE_MANAGER with function registry
        new FunctionRegistry(TYPE_MANAGER, new BlockEncodingManager(TYPE_MANAGER), new FeaturesConfig());
    }
}
