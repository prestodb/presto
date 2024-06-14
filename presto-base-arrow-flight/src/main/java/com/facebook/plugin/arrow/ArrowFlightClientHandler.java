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
package com.facebook.plugin.arrow;

import com.facebook.airlift.log.Logger;
import com.facebook.presto.spi.ConnectorSession;
import org.apache.arrow.flight.FlightClient;
import org.apache.arrow.flight.FlightDescriptor;
import org.apache.arrow.flight.FlightInfo;
import org.apache.arrow.flight.Location;
import org.apache.arrow.flight.auth2.BearerCredentialWriter;
import org.apache.arrow.flight.grpc.CredentialCallOption;
import org.apache.arrow.memory.RootAllocator;

import java.io.FileInputStream;
import java.io.IOException;
import java.io.InputStream;
import java.util.Optional;
import java.util.Timer;
import java.util.TimerTask;
import java.util.concurrent.atomic.AtomicBoolean;

import static com.facebook.plugin.arrow.ArrowErrorCode.ARROW_FLIGHT_ERROR;

public abstract class ArrowFlightClientHandler
{
    private static final Logger logger = Logger.get(ArrowFlightClientHandler.class);
    private AtomicBoolean isClientClosed = new AtomicBoolean(true);
    private final ArrowClientAuth arrowClientAuth;
    private final ArrowFlightConfig config;
    private Timer timer = new Timer(true);
    private Optional<String> bearerToken = Optional.empty();
    private ArrowFlightClient arrowFlightClient;
    private Optional<InputStream> trustedCertificate = Optional.empty();
    private TimerTask closeTask;
    private static final int TIMER_DURATION_IN_MINUTES = 30;

    public ArrowFlightClientHandler(ArrowFlightConfig config, ArrowClientAuth arrowClientAuth)
    {
        this.config = config;
        this.arrowClientAuth = arrowClientAuth;
    }

    private void initializeClient(Optional<String> uri)
    {
        if (!isClientClosed.get()) {
            return;
        }
        try {
            RootAllocator allocator = new RootAllocator(Long.MAX_VALUE);
            Optional<InputStream> trustedCertificate = Optional.empty();

            Location location;
            if (uri.isPresent()) {
                location = new Location(uri.get());
            }
            else {
                if (config.getArrowFlightServerSslEnabled() != null && !config.getArrowFlightServerSslEnabled()) {
                    location = Location.forGrpcInsecure(config.getFlightServerName(), config.getArrowFlightPort());
                }
                else {
                    location = Location.forGrpcTls(config.getFlightServerName(), config.getArrowFlightPort());
                }
            }

            logger.debug("location %s", location.getUri().toString());

            FlightClient.Builder flightClientBuilder = FlightClient.builder(allocator, location);
            if (config.getVerifyServer() != null && !config.getVerifyServer()) {
                flightClientBuilder.verifyServer(false);
            }
            else if (config.getFlightServerSSLCertificate() != null) {
                trustedCertificate = Optional.of(new FileInputStream(config.getFlightServerSSLCertificate()));
                flightClientBuilder.trustedCertificates(trustedCertificate.get()).useTls();
            }

            FlightClient flightClient = flightClientBuilder.build();
            this.arrowFlightClient = new ArrowFlightClient(flightClient, trustedCertificate);
            isClientClosed.set(false);
        }
        catch (Exception ex) {
            throw new ArrowException(ARROW_FLIGHT_ERROR, "The flight client could not be obtained." + ex.getMessage(), ex);
        }
    }

    public ArrowFlightConfig getConfig()
    {
        return config;
    }

    public ArrowFlightClient getClient(Optional<String> uri)
    {
        if (isClientClosed.get()) { // Check if client is closed or not initialized
            logger.info("Reinitialize the client if closed or not initialized");
            initializeClient(uri);
            scheduleCloseTask();
        }
        else {
            resetTimer(); // Reset timer when client is reused
        }
        return this.arrowFlightClient;
    }

    public FlightInfo getFlightInfo(ArrowFlightRequest request, ConnectorSession connectorSession, Optional<String> bearerToken)
    {
        try {
            ArrowFlightClient client = getClient(Optional.empty());
            CredentialCallOption auth = this.getCallOptions(connectorSession, bearerToken);
            FlightDescriptor descriptor = FlightDescriptor.command(request.getCommand());
            logger.debug("Fetching flight info");
            FlightInfo flightInfo = client.getFlightClient().getInfo(descriptor, ArrowFlightConstants.CALL_OPTIONS_TIMEOUT, auth);
            logger.debug("got flight info");
            return flightInfo;
        }
        catch (Exception e) {
            throw new ArrowException(ARROW_FLIGHT_ERROR, "The flight information could not be obtained from the flight server." + e.getMessage(), e);
        }
    }

    public CredentialCallOption getCallOptions(ConnectorSession connectorSession, Optional<String> bearerToken) throws IOException
    {
        if (bearerToken.isPresent()) {
            return new CredentialCallOption(new BearerCredentialWriter(bearerToken.get()));
        }
        else {
            // If no token was passed in, generate a new token.
            this.bearerToken = Optional.of(arrowClientAuth.getBearerToken(connectorSession));
            return new CredentialCallOption(new BearerCredentialWriter(this.bearerToken.get()));
        }
    }

    public Optional<String> getBearerToken()
    {
        return bearerToken;
    }

    public void close() throws Exception
    {
        if (arrowFlightClient != null) {
            arrowFlightClient.close();
            arrowFlightClient = null;
        }
        if (trustedCertificate.isPresent()) {
            trustedCertificate.get().close();
        }
        shutdownTimer();
        isClientClosed.set(true);
    }

    private void scheduleCloseTask()
    {
        cancelCloseTask();
        closeTask = new TimerTask() {
            @Override
            public void run()
            {
                try {
                    close();
                }
                catch (Exception e) {
                    logger.error(e);
                }
            }
        };
        if (timer == null) {
            timer = new Timer(true);
        }
        timer.schedule(closeTask, TIMER_DURATION_IN_MINUTES * 60 * 1000);
    }

    private void cancelCloseTask()
    {
        if (closeTask != null) {
            closeTask.cancel();
        }
    }

    public void resetTimer()
    {
        shutdownTimer();
        scheduleCloseTask();
    }

    public void shutdownTimer()
    {
        if (timer != null) {
            timer.cancel();
            timer = null;
        }
    }
}
