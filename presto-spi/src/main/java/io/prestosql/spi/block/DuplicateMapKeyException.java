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
package io.prestosql.spi.block;

import io.prestosql.spi.connector.ConnectorSession;
import io.prestosql.spi.type.Type;

import static java.lang.String.format;

public class DuplicateMapKeyException
        extends Exception
{
    private final Block block;
    private final int position;

    public DuplicateMapKeyException(Block block, int position)
    {
        super("Duplicate map keys are not allowed");
        this.block = block;
        this.position = position;
    }

    public String getDetailedMessage(Type keyType, ConnectorSession session)
    {
        return format("Duplicate map keys (%s) are not allowed", keyType.getObjectValue(session, block, position));
    }
}
