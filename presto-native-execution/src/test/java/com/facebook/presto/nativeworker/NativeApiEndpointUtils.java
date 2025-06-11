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
package com.facebook.presto.nativeworker;

import java.io.BufferedReader;
import java.io.InputStreamReader;
import java.net.HttpURLConnection;
import java.net.URL;
import java.util.HashMap;
import java.util.Map;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

public final class NativeApiEndpointUtils
{
    private NativeApiEndpointUtils() {}
    public static Map<String, Long> fetchScalarLongMetrics(String serverAndPort, String apiEndpoint, String requestMethod) throws Exception
    {
        String apiUrl = serverAndPort + apiEndpoint;
        URL url = new URL(apiUrl);
        HttpURLConnection connection = (HttpURLConnection) url.openConnection();
        connection.setRequestMethod(requestMethod);
        connection.setConnectTimeout(5000);
        connection.setReadTimeout(5000);

        Map<String, Long> metrics = new HashMap<>();
        Pattern metricPattern = Pattern.compile("^(\\w+)(?:\\{.*?\\})?\\s+(\\d+)$");

        try (BufferedReader reader = new BufferedReader(new InputStreamReader(connection.getInputStream()))) {
            String line;
            while ((line = reader.readLine()) != null) {
                Matcher matcher = metricPattern.matcher(line);
                if (matcher.matches()) {
                    String metricName = matcher.group(1);
                    long metricValue = Long.parseLong(matcher.group(2));
                    metrics.put(metricName, metricValue);
                }
            }
        }
        finally {
            connection.disconnect();
        }

        return metrics;
    }

    public static int sendWorkerRequest(String serverAndPort, String endPoint)
    {
        try {
            URL url = new URL(serverAndPort + endPoint);
            HttpURLConnection connection = (HttpURLConnection) url.openConnection();
            connection.setRequestMethod("GET");
            connection.setRequestProperty("Accept", "application/json");
            return connection.getResponseCode();
        }
        catch (Exception e) {
            e.printStackTrace();
            return 500;
        }
    }
}
