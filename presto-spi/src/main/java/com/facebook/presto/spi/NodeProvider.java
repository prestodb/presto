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
package com.facebook.presto.spi;

import java.util.List;

public interface NodeProvider
{
    /**
     * @param identifier   an unique identifier used to obtain the nodes
     * @param count        how many desirable nodes to be returned
     * @return a list of the chosen nodes by specific hash function
     */
    List<HostAddress> get(String identifier, int count);
}
