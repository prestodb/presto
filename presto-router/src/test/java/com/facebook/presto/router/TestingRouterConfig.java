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
package com.facebook.presto.router;

import org.json.simple.JSONArray;
import org.json.simple.JSONObject;

import java.util.List;
import java.util.Map;

public class TestingRouterConfig
{
    private JSONObject config;

    public TestingRouterConfig(Map<String, List<String>> groups, String targetGroup, String scheduler)
    {
        JSONArray groupsArray = new JSONArray();
        groups.forEach((name, members) -> {
            JSONArray membersArray = new JSONArray();
            membersArray.addAll(members);
            JSONObject group = new JSONObject();
            group.put("name", name);
            group.put("members", membersArray);
            groupsArray.add(group);
        });

        JSONArray selectorsArray = new JSONArray();
        JSONObject selector = new JSONObject();
        selector.put("targetGroup", targetGroup);
        selectorsArray.add(selector);

        config = new JSONObject();
        config.put("groups", groupsArray);
        config.put("selectors", selectorsArray);
        config.put("scheduler", scheduler);
    }

    public JSONObject get()
    {
        return config;
    }
}
