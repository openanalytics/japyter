/*******************************************************************************
 * Copyright (c) 2015-2016 Open Analytics NV and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package eu.openanalytics.japyter;

import static org.apache.commons.lang3.Validate.notNull;

import java.io.Closeable;
import java.io.File;
import java.io.IOException;
import java.net.URI;

import org.apache.commons.codec.digest.HmacAlgorithms;
import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import com.fasterxml.jackson.databind.ObjectMapper;

import eu.openanalytics.japyter.client.Control;
import eu.openanalytics.japyter.client.Heartbeat;
import eu.openanalytics.japyter.client.IoPub;
import eu.openanalytics.japyter.client.Protocol;
import eu.openanalytics.japyter.client.Session;
import eu.openanalytics.japyter.client.Shell;
import eu.openanalytics.japyter.client.Stdin;
import eu.openanalytics.japyter.client.Stdin.StdinHandler;
import eu.openanalytics.japyter.model.gen.Config;
import eu.openanalytics.japyter.model.gen.ConnectReply;

/**
 * The main entry point to <b>Japyter</b>, a Java client library for Jupyter. <b>Japyter</b>
 * implements the <a href="http://ipython.org/ipython-doc/stable/development/messaging.html">iPython
 * messaging protocol</a>.
 */
public final class Japyter implements Closeable
{
    public static final class Builder
    {
        private final Config config;
        private String userName;
        private int receiveTimeoutMillis = 3000; // good old Erlang default timeout
        private StdinHandler stdinHandler;
        private int heartbeatPeriodMillis = 10000;
        private int zmqIoThreads = 1;

        private Builder(final Config config)
        {
            this.config = config;
        }

        /**
         * The user name for the newly created session. Optional.
         */
        public Builder withUserName(final String userName)
        {
            this.userName = userName;
            return this;
        }

        /**
         * Set the timeout (in milliseconds) to use when blocked on a receive operation. 0 means no
         * wait and -1 means wait for ever (the latter is usually a very bad idea). Defaults to
         * 3000.
         */
        public Builder withReceiveTimeout(final int receiveTimeoutMillis)
        {
            this.receiveTimeoutMillis = receiveTimeoutMillis;
            return this;
        }

        /**
         * An implementation of {@link StdinHandler} to deal with input requests from the kernel.
         * Optional.
         */
        public Builder withStdinHandler(final StdinHandler stdinHandler)
        {
            this.stdinHandler = stdinHandler;
            return this;
        }

        /**
         * Set the period (in milliseconds) at which the client pings the kernel over the hearbeat
         * channel. 0 disables the features. Defaults to 10000.
         */
        public Builder withHeartbeatPeriodMillis(final int heartbeatPeriodMillis)
        {
            this.heartbeatPeriodMillis = heartbeatPeriodMillis;
            return this;
        }

        /**
         * Set the number of I/O threads available to the ZeroMQ context. 1 is the minimum. Defaults
         * to 1.
         */
        public Builder withZmqIoThreads(final int zmqIoThreads)
        {
            this.zmqIoThreads = zmqIoThreads;
            return this;
        }

        /**
         * Builds a new {@link Japyter} instance and an associated client session.
         *
         * @return
         */
        public Japyter build()
        {
            return new Japyter(config, userName, receiveTimeoutMillis, stdinHandler, heartbeatPeriodMillis,
                zmqIoThreads);
        }
    }

    public static final ObjectMapper JSON_OBJECT_MAPPER = new ObjectMapper();

    private static final Logger LOGGER = LoggerFactory.getLogger(Japyter.class);

    private final Config config;
    private final Session session;
    private final Shell shell;
    private final Control control;
    private final IoPub ioPub;
    private final Stdin stdin;
    private final Heartbeat heartbeat;

    private Japyter(final Config config,
                    final String userName,
                    final int receiveTimeoutMillis,
                    final StdinHandler stdinHandler,
                    final int heartbeatPeriodMillis,
                    final int zmqIoThreads)
    {
        this.config = notNull(config, "config can't be null");

        Validate.notBlank(config.getTransport(), "transport can't be empty");
        Validate.notBlank(config.getIp(), "ip can't be empty");

        final Protocol protocol = new Protocol(config.getKey(), config.getSignatureScheme());

        session = new Session(userName, protocol, receiveTimeoutMillis, zmqIoThreads);

        shell = config.getShellPort() != null ? new Shell(getChannelAddress(config.getShellPort(), config),
            session) : null;

        control = config.getControlPort() != null ? new Control(getChannelAddress(config.getControlPort(),
            config), session) : null;

        ioPub = config.getIopubPort() != null ? new IoPub(getChannelAddress(config.getIopubPort(), config),
            session) : null;

        if (config.getStdinPort() != null)
        {
            if (stdinHandler != null)
            {
                stdin = new Stdin(getChannelAddress(config.getStdinPort(), config), session, stdinHandler);
            }
            else
            {
                LOGGER.info("No Stdin handler has been configured although the kernel is configured with a stdin port");
                stdin = null;
            }
        }
        else
        {
            stdin = null;
        }

        heartbeat = config.getHbPort() != null ? new Heartbeat(getChannelAddress(config.getHbPort(), config),
            session, heartbeatPeriodMillis) : null;
    }

    private String getChannelAddress(final Integer channelPort, final Config config)
    {
        return config.getTransport() + "://" + config.getIp() + ":" + channelPort;
    }

    /**
     * The {@link Config} that was load when this instance of {@link Japyter} has been created.
     * Mutating its values has no effect.
     */
    public Config getConfig()
    {
        return config;
    }

    @Override
    public void close() throws IOException
    {
        session.close();
    }

    /**
     * Creates a new {@link Builder} instance for configuring and instantiating a new
     * {@link Japyter} instance.
     *
     * @param jsonConfigLocation the location of the iPython networking and security JSON
     *            configuration.
     * @throws IOException in case anything goes wrong when loading the configuration.
     */
    public static Builder fromConfigFile(final File jsonConfigLocation) throws IOException
    {
        LOGGER.info("Loading configuration: {}", jsonConfigLocation);
        final Config config = JSON_OBJECT_MAPPER.readValue(jsonConfigLocation, Config.class);
        return new Builder(config);
    }

    /**
     * Creates a new {@link Builder} instance for configuring and instantiating a new
     * {@link Japyter} instance.
     *
     * @param config the iPython networking and security configuration.
     * @throws IOException in case anything goes wrong when loading the configuration.
     */
    public static Builder fromConfig(final Config config) throws IOException
    {
        return new Builder(config);
    }

    /**
     * Creates a new {@link Builder} instance for configuring and instantiating a new
     * {@link Japyter} instance, which first retrieves all the connection information from a call (
     * <code>connect_request</code> request) to the provided control or shell ZeroMQ router. No HMAC
     * signature is used for this call.
     *
     * @param controlOrShell a {@link URI} that points to either a shell or control ZeroMQ router.
     * @throws IOException in case anything goes wrong when retrieving the network configuration.
     */
    public static Builder fromUri(final URI controlOrShell) throws IOException
    {
        return fromUri(controlOrShell, null, null);
    }

    /**
     * Creates a new {@link Builder} instance for configuring and instantiating a new
     * {@link Japyter} instance, which first retrieves all the connection information from a call (
     * <code>connect_request</code> request) to the provided control or shell ZeroMQ router.
     *
     * @param controlOrShell a {@link URI} that points to either a shell or control ZeroMQ router.
     * @param hmacKey the request HMAC signing key, or null if request signature is disabled on the
     *            kernel.
     * @param hmacAlgorithm the request HMAC signing algorithm, or null if request signature is
     *            disabled on the kernel.
     * @throws IOException in case anything goes wrong when retrieving the network configuration.
     */
    public static Builder fromUri(final URI controlOrShell,
                                  final byte[] hmacKey,
                                  final HmacAlgorithms hmacAlgorithm) throws IOException
    {
        LOGGER.info("Fetching connection information from: {}",
            notNull(controlOrShell, "controlOrShell can't be null"));

        final Protocol protocol = new Protocol(hmacKey, hmacAlgorithm);

        try (Session tempSession = new Session(Japyter.class.getName(), protocol, 3000, 1))
        {
            final Shell tempShell = new Shell(controlOrShell.toString(), tempSession);
            final ConnectReply connectReply = tempShell.connect();

            final Config config = new Config();

            config.withControlPort(connectReply.getControl())
                .withHbPort(connectReply.getHb())
                .withIopubPort(connectReply.getIopub())
                .withIp(controlOrShell.getHost())
                .withKey(hmacKey != null ? new String(hmacKey, Protocol.ENCODING) : null)
                .withShellPort(connectReply.getShell())
                .withSignatureScheme(hmacAlgorithm != null ? hmacAlgorithm.toString() : null)
                .withStdinPort(connectReply.getStdin())
                .withTransport(controlOrShell.getScheme());

            LOGGER.info("Connection information received: {}, losing temporary session", connectReply);

            return fromConfig(config);
        }
    }

    /**
     * @return the active {@link Session} for this {@link Japyter} instance.
     */
    public Session getSession()
    {
        return session;
    }

    /**
     * @return the {@link Shell} client, or null if no port was configured for it.
     */
    public Shell getShell()
    {
        return shell;
    }

    /**
     * @return the {@link Control} client, or null if no port was configured for it.
     */
    public Control getControl()
    {
        return control;
    }

    /**
     * @return the {@link IoPub} client, or null if no port was configured for it.
     */
    public IoPub getIoPub()
    {
        return ioPub;
    }

    /**
     * @return the {@link Stdin} client, or null if no port was configured for it or if no
     *         {@link StdinHandler} has been configured.
     */
    public Stdin getStdin()
    {
        return stdin;
    }

    /**
     * @return the {@link Heartbeat} client, or null if no port was configured for it.
     */
    public Heartbeat getHeartbeat()
    {
        return heartbeat;
    }
}
