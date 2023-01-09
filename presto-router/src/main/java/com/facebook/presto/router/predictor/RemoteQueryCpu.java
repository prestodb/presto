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
package com.facebook.presto.router.predictor;

import com.facebook.airlift.http.client.HttpClient;
import com.facebook.airlift.log.Logger;
import com.fasterxml.jackson.databind.JsonNode;
import com.fasterxml.jackson.databind.ObjectMapper;

import javax.annotation.concurrent.ThreadSafe;

import java.net.URI;
import java.util.Map;

import static com.facebook.airlift.http.client.HttpStatus.OK;

@ThreadSafe
public class RemoteQueryCpu
        extends RemoteQuery
{
    private static final Logger log = Logger.get(RemoteQueryCpu.class);
    private static final ObjectMapper mapper = new ObjectMapper();

    private static final String CPU_TIME_LABEL = "cpu_pred_label";
    private static final String CPU_TIME_STR = "cpu_pred_str";

    private CpuInfo cpuInfo;

    public RemoteQueryCpu(HttpClient httpClient, URI remoteUri)
    {
        super(httpClient, remoteUri);
    }

    @Override
    public void handleResponse(JsonNode response)
    {
        try {
            Map<String, Object> fields = mapper.convertValue(response, Map.class);
            if (fields.containsKey("status") && (int) fields.get("status") != OK.code()) {
                cpuInfo = null;
                return;
            }
            cpuInfo = new CpuInfo((int) fields.get(CPU_TIME_LABEL), (String) fields.get(CPU_TIME_STR));
        }
        catch (Exception e) {
            log.error("Error handling response: %s", response.toString());
        }
    }

    public CpuInfo getCpuInfo()
    {
        return cpuInfo;
    }
}
