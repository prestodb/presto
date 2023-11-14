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
package io.ahana.eventplugin;

import okhttp3.OkHttpClient;
import okhttp3.OkHttpClient.Builder;
import okhttp3.Request;
import okhttp3.Response;
import okhttp3.WebSocket;
import okhttp3.WebSocketListener;

import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.TimeUnit;

public final class WebSocketCollectorChannel
        extends WebSocketListener
{
    private final String url;
    private double backoff;
    private WebSocket webSocket;

    public WebSocketCollectorChannel(String url)
    {
        super();
        this.url = url;
    }

    private void reconnect()
    {
        (new Timer())
                .schedule(
                        new TimerTask()
                        {
                            public void run()
                            {
                                WebSocketCollectorChannel.this.connect();
                            }
                        },
                        (long) this.backoff);
    }

    public final void connect()
    {
        OkHttpClient client = (
                new Builder())
                .readTimeout(60000L, TimeUnit.MILLISECONDS)
                .pingInterval(300, TimeUnit.SECONDS)
                .retryOnConnectionFailure(true)
                .build();
        Request request = (new okhttp3.Request.Builder()).url(this.url).build();
        client.newWebSocket(request, this);

        this.backoff = Math.min(300000.0, Math.max(1000.0, this.backoff * 1.25));
    }

    public final void sendMessage(String text)
    {
        WebSocket webSocket = this.webSocket;
        if (webSocket != null) {
            webSocket.send(text);
        }
    }

    public void onOpen(WebSocket webSocket, Response response)
    {
        this.webSocket = webSocket;
        this.backoff = 0;
    }

    private void close()
    {
        WebSocket webSocket = this.webSocket;
        if (webSocket != null) {
            webSocket.close(1000, null);
        }
        this.webSocket = null;
    }

    public void onClosing(WebSocket webSocket, int code, String reason)
    {
        this.close();
        this.reconnect();
    }

    public void onFailure(WebSocket webSocket, Throwable t, Response response)
    {
        this.close();
        this.reconnect();
    }
}
