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

package com.facebook.presto.resourceGroups.reloading;

import com.facebook.presto.resourceGroups.ManagerSpec;
import com.facebook.presto.resourceGroups.ResourceGroupSelector;

import java.util.List;

/**
 * Provides the ReloadingResourceGroupConfigurationManager class with an updated ManagerSpec with the base
 * resource group configuration data. These methods can be implemented to retrieve data from various
 * data sources.
 */

public interface ManagerSpecProvider
{
    ManagerSpec getManagerSpec();

    List<ResourceGroupSelector> getExactMatchSelectors();
}
