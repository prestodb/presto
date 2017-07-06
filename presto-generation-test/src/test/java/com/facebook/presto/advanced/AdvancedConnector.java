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
package com.facebook.presto.advanced;

import com.facebook.presto.framework.Connector;

public class AdvancedConnector
        implements Connector
{
    @Override
    public void dataOnlyFeature()
    {
        System.out.println("AdvancedConnector dataOnlyFeature");
    }

    @Override
    public void advancedFeature()
    {
        System.out.println("AdvancedConnector advancedFeature");
    }
}
