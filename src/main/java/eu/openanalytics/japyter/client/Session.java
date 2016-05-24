/*******************************************************************************
 * Copyright (c) 2015-2016 Open Analytics NV and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package eu.openanalytics.japyter.client;

import static java.util.concurrent.TimeUnit.MILLISECONDS;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.Validate.notNull;

import java.io.Closeable;
import java.io.IOException;
import java.util.ArrayList;
import java.util.HashSet;
import java.util.List;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.ScheduledExecutorService;
import java.util.concurrent.ThreadFactory;
import java.util.concurrent.atomic.AtomicInteger;

import org.apache.commons.lang3.Validate;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZContext;
import org.zeromq.ZMQ.Socket;

import eu.openanalytics.japyter.Japyter;
import eu.openanalytics.japyter.model.Message;

public class Session implements Closeable
{
    private static class SessionThreadFactory implements ThreadFactory
    {
        private final AtomicInteger count = new AtomicInteger(0);

        @Override
        public Thread newThread(final Runnable r)
        {
            final Thread t = new Thread(r);
            t.setName((Japyter.class.getSimpleName() + "-" + r.getClass().getSimpleName() + "-" + count.incrementAndGet()).toLowerCase());
            return t;
        }
    }

    private static final Logger LOGGER = LoggerFactory.getLogger(Session.class);
    private static final long SHUTDOWN_TIMEOUT_PADDING = 3000L;

    private final String id, userName;
    private final Protocol protocol;
    private final int receiveTimeoutMillis;
    private final ZContext zmqContext;
    private final Set<AbstractChannel> channels;
    private final ExecutorService executor;
    private final ScheduledExecutorService scheduler;

    public Session(final String userName,
                   final Protocol protocol,
                   final int receiveTimeoutMillis,
                   final int zmqIoThreads)
    {
        id = UUID.randomUUID().toString();

        this.userName = userName;
        this.protocol = notNull(protocol, "protocol can't be null");

        Validate.isTrue(receiveTimeoutMillis >= -1,
            "receiveTimeoutMillis must be -1, 0 or a positive integer");
        this.receiveTimeoutMillis = receiveTimeoutMillis;

        Validate.isTrue(zmqIoThreads >= 1, "zmqIoThreads must be a positive integer");
        zmqContext = new ZContext(zmqIoThreads);

        channels = new HashSet<>();

        executor = Executors.newCachedThreadPool(new SessionThreadFactory());
        scheduler = Executors.newScheduledThreadPool(1, new SessionThreadFactory());

        LOGGER.info("Created session ID is: {}", id);
    }

    public String getUserName()
    {
        return userName;
    }

    public Protocol getProtocol()
    {
        return protocol;
    }

    public int getReceiveTimeoutMillis()
    {
        return receiveTimeoutMillis;
    }

    protected Socket connect(final AbstractChannel channel)
    {
        final Socket zmqSocket = zmqContext.createSocket(channel.getZmqSocketType());
        zmqSocket.setLinger(1000L);
        zmqSocket.setReceiveTimeOut(receiveTimeoutMillis);
        zmqSocket.connect(channel.getAddress());

        channels.add(channel);
        LOGGER.info("Connected channel: {} {}", channel.getClass().getSimpleName(), channel.getAddress());

        return zmqSocket;
    }

    protected void disconnect(final AbstractChannel channel)
    {
        zmqContext.destroySocket(channel.getZmqSocket());
    }

    protected void execute(final Runnable r)
    {
        executor.execute(r);
    }

    protected void scheduleWithFixedDelay(final Runnable r, final int delayMillis)
    {
        scheduler.scheduleWithFixedDelay(r, 0L, delayMillis, MILLISECONDS);
    }

    private static void shutdownExecutor(final ExecutorService es, final long timeout)
    {
        try
        {
            es.shutdown();

            LOGGER.info("Shutdown requested for {}, awaiting for a maximum of {}ms", es, timeout);

            if (!es.awaitTermination(timeout, MILLISECONDS))
            {
                final List<Runnable> dumpedRunnables = es.shutdownNow();
                LOGGER.error("Shutdown of {} timed-out, dumped runnables: {}", es, dumpedRunnables);
            }
        }
        catch (final InterruptedException ie)
        {
            // Restore the interrupted status
            Thread.currentThread().interrupt();
        }
    }

    @Override
    public void close() throws IOException
    {
        for (final AbstractChannel channel : channels)
        {
            if (channel instanceof AbstractRunningChannel)
            {
                try
                {
                    LOGGER.info("Stopping channel: {} {}", channel.getClass().getSimpleName(),
                        channel.getAddress());

                    ((AbstractRunningChannel) channel).stop();
                }
                catch (final Throwable t)
                {
                    LOGGER.warn("Failed to cleanly stop {}", channel, t);
                }
            }
        }

        final long shutdownExecutorTimeout = SHUTDOWN_TIMEOUT_PADDING + getReceiveTimeoutMillis();
        shutdownExecutor(executor, shutdownExecutorTimeout);
        shutdownExecutor(scheduler, shutdownExecutorTimeout);

        for (final AbstractChannel channel : channels)
        {
            try
            {
                LOGGER.info("Closing channel: {} {}", channel.getClass().getSimpleName(),
                    channel.getAddress());

                channel.close();
            }
            catch (final Throwable t)
            {
                LOGGER.warn("Failed to cleanly close {}", channel, t);
            }
        }

        zmqContext.destroy();

        LOGGER.info("Terminated session ID is: {}", id);
    }

    protected void send(final Message message, final Socket zmqSocket) throws IOException
    {
        message.getHeader().setSession(id);

        if (isNotBlank(userName))
        {
            message.getHeader().setUsername(userName);
        }

        final List<byte[]> frames = protocol.toFrames(message);
        final int framesSize = frames.size();

        for (int i = 0; i < framesSize; i++)
        {
            final boolean lastFrame = i == (framesSize - 1);

            final byte[] frame = frames.get(i);

            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("Sending {}frame: {}", (lastFrame ? "last " : ""), new String(frame,
                    Protocol.ENCODING));
            }

            if (lastFrame)
            {
                if (!zmqSocket.send(frame))
                {
                    throw new IOException("Failed to send frame " + i + " of message: " + message);
                }
            }
            else
            {
                if (!zmqSocket.sendMore(frame))
                {
                    throw new IOException("Failed to send frame " + i + " of message: " + message);
                }
            }
        }
    }

    public Message receive(final Socket zmqSocket) throws IOException
    {
        return receive(zmqSocket, true);
    }

    public Message poll(final Socket zmqSocket) throws IOException
    {
        return receive(zmqSocket, false);
    }

    private Message receive(final Socket zmqSocket, final boolean failOnNull) throws IOException
    {
        byte[] frame = zmqSocket.recv();

        if (frame == null)
        {
            if (failOnNull)
            {
                throw new IOException("Received null first frame after waiting " + receiveTimeoutMillis
                                      + "ms");
            }
            else
            {
                return null;
            }
        }

        final List<byte[]> frames = new ArrayList<>();

        do
        {
            frames.add(frame);
        }
        while (zmqSocket.hasReceiveMore() && ((frame = zmqSocket.recv()) != null));

        return protocol.fromFrames(frames);
    }
}
