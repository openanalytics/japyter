/*******************************************************************************
 * Copyright (c) 2015-2016 Open Analytics NV and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package eu.openanalytics.japyter.client;

import static eu.openanalytics.japyter.Japyter.JSON_OBJECT_MAPPER;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;

import java.io.IOException;
import java.util.List;
import java.util.concurrent.CopyOnWriteArrayList;

import org.zeromq.ZMQ;

import eu.openanalytics.japyter.client.Protocol.BroadcastType;
import eu.openanalytics.japyter.model.Message;
import eu.openanalytics.japyter.model.gen.Broadcast;

public class IoPub extends AbstractRunningChannel
{
    private class IoPubPoller implements Runnable
    {
        @Override
        public void run()
        {
            while (isRunning())
            {
                try
                {
                    pollAndRoute();
                }
                catch (final Throwable t)
                {
                    if (isRunning())
                    {
                        getLogger().error("Polling and routing to subscriber failed", t);
                    }
                }
            }

            getLogger().info("Stopped");
        }
    }

    public interface Listener
    {
        // marker interface
    }

    public interface BroadcastListener extends Listener
    {
        void handle(Broadcast b);
    }

    public interface MessageListener extends Listener
    {
        void handle(Message m);
    }

    private final List<Listener> listeners;

    public IoPub(final String address, final Session session)
    {
        super(address, session, false);

        listeners = new CopyOnWriteArrayList<>();

        getZmqSocket().subscribe(EMPTY_BYTE_ARRAY);
    }

    @Override
    protected int getZmqSocketType()
    {
        return ZMQ.SUB;
    }

    public synchronized void subscribe(final BroadcastListener broadcastListener)
    {
        doSubscribe(broadcastListener);
    }

    public synchronized void subscribe(final MessageListener messageListener)
    {
        doSubscribe(messageListener);
    }

    private synchronized void doSubscribe(final Listener listener)
    {
        listeners.add(listener);

        if (isRunning())
        {
            return;
        }

        start();

        getSession().execute(new IoPubPoller());
    }

    private void pollAndRoute() throws IOException
    {
        final Message maybeMessage = getSession().poll(getZmqSocket());

        if (maybeMessage == null)
        {
            return;
        }

        for (final Listener listener : listeners)
        {
            route(maybeMessage, listener);
        }
    }

    private void route(final Message message, final Listener listener)
    {
        if (listener instanceof MessageListener)
        {
            ((MessageListener) listener).handle(message);
        }
        else
        {
            final Class<? extends Broadcast> broadcastClass = BroadcastType.classFromValue(message.getHeader()
                .getMsgType());

            if (getLogger().isDebugEnabled())
            {
                getLogger().debug("Selected broadcast class: {} for message: {}", broadcastClass, message);
            }

            final Broadcast broadcast = JSON_OBJECT_MAPPER.convertValue(message.getContent(), broadcastClass);

            ((BroadcastListener) listener).handle(broadcast);
        }
    }
}
