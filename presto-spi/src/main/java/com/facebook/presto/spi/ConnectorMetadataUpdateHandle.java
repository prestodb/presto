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

/**
 * ConnectorMetadataUpdateHandle is the basic unit of metadata that is used to represent
 * both the request and the response in the metadata update communication cycle
 * between the worker and the coordinator.
 *
 * Every connector that supports sending/receiving the metadata requests/responses
 * will implement this interface.
 */
public interface ConnectorMetadataUpdateHandle
{
}
