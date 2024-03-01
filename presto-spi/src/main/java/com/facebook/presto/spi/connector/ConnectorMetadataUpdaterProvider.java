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
package com.facebook.presto.spi.connector;

/**
 * Every connector that support sending metadata updates to coordinator will
 * have a SINGLETON provider instance that implements this interface.
 * <p>
 * This provider will be used to create a new instance of ConnectorMetadataUpdater
 * for every table writer operator in a Task.
 */
public interface ConnectorMetadataUpdaterProvider
{
    /**
     * Create and return metadata updater that handles metadata requests/responses.
     *
     * @return metadata updater
     */
    ConnectorMetadataUpdater getMetadataUpdater();
}
