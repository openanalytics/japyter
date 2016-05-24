/*******************************************************************************
 * Copyright (c) 2015-2016 Open Analytics NV and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package eu.openanalytics.japyter.client;

import static eu.openanalytics.japyter.client.Heartbeat.State.DOWN;
import static eu.openanalytics.japyter.client.Heartbeat.State.UNKNOWN;
import static eu.openanalytics.japyter.client.Heartbeat.State.UP;
import static java.util.UUID.randomUUID;

import java.util.concurrent.atomic.AtomicReference;

import org.zeromq.ZMQ;

public class Heartbeat extends AbstractRunningChannel
{
    private class HeartbeatPinger implements Runnable
    {
        @Override
        public void run()
        {
            if (!isRunning())
            {
                return;
            }

            try
            {
                ping();
            }
            catch (final Throwable t)
            {
                if (isRunning())
                {
                    getLogger().error("Heartbeat ping failed", t);
                }
            }
        }
    }

    public enum State
    {
        UNKNOWN, UP, DOWN
    };

    private final AtomicReference<State> stateRef = new AtomicReference<State>(UNKNOWN);

    public Heartbeat(final String address, final Session session, final int heartbeatPeriodMillis)
    {
        super(address, session, heartbeatPeriodMillis > 0);

        if (isRunning())
        {
            getLogger().info("Scheduling hearbeat pinger every {}ms", heartbeatPeriodMillis);
            getSession().scheduleWithFixedDelay(new HeartbeatPinger(), heartbeatPeriodMillis);
        }
        else
        {
            getLogger().info("Hearbeat pinger is disabled");
        }
    }

    public State getState()
    {
        return stateRef.get();
    }

    @Override
    protected int getZmqSocketType()
    {
        return ZMQ.REQ;
    }

    private void ping()
    {
        final String pingPayload = randomUUID().toString();
        getZmqSocket().send(pingPayload);
        final String echoedPayload = getZmqSocket().recvStr();

        getLogger().debug("Sent ping payload: {}, received back: {}", pingPayload, echoedPayload);

        if (echoedPayload == null)
        {
            stateRef.set(DOWN);

            throw new IllegalStateException("Kernel heartbeat failed");
        }
        else if (!pingPayload.equals(echoedPayload))
        {
            stateRef.set(UNKNOWN);

            throw new IllegalArgumentException("Kernel hearbeat is inconsistent");
        }
        else
        {
            stateRef.set(UP);
        }
    }
}
