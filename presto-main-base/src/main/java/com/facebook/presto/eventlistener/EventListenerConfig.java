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

package com.facebook.presto.eventlistener;

import com.facebook.airlift.configuration.Config;
import com.google.common.base.Splitter;
import com.google.common.collect.ImmutableList;

import javax.validation.constraints.NotNull;

import java.io.File;
import java.util.List;

import static com.google.common.collect.ImmutableList.toImmutableList;

public class EventListenerConfig
{
    private static final Splitter SPLITTER = Splitter.on(',').trimResults().omitEmptyStrings();
    private List<File> eventListenerFiles = ImmutableList.of();

    @NotNull
    public List<File> getEventListenerFiles()
    {
        return eventListenerFiles;
    }

    @Config("event-listener.config-files")
    public EventListenerConfig setEventListenerFiles(String eventListenerFiles)
    {
        this.eventListenerFiles = SPLITTER.splitToList(eventListenerFiles).stream()
                .map(File::new)
                .collect(toImmutableList());
        return this;
    }

    public EventListenerConfig setEventListenerFiles(List<File> eventListenerFiles)
    {
        this.eventListenerFiles = ImmutableList.copyOf(eventListenerFiles);
        return this;
    }
}
