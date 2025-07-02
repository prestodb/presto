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

import io.ahana.eventplugin.QueryEventListenerFactory;
import org.junit.jupiter.api.Test;

import java.util.HashMap;
import java.util.Map;

import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertThrows;

public class QueryEventListenerFactoryTest
{
    @Test
    public void getName()
    {
        QueryEventListenerFactory listenerFactory = new QueryEventListenerFactory();
        assertEquals("watsonx.data-listener", listenerFactory.getName());
    }

    @Test
    public void createWithoutConfigShouldThrowException()
    {
        // Given
        Map<String, String> configs = new HashMap<>();
        configs.put(QueryEventListenerFactory.QUERYEVENT_CONFIG_LOCATION, null);
        // When
        QueryEventListenerFactory listenerFactory = new QueryEventListenerFactory();
        // Then
        assertThrows(
                NullPointerException.class,
                () -> listenerFactory.create(configs),
                QueryEventListenerFactory.QUERYEVENT_CONFIG_LOCATION + " is null");
    }
}
