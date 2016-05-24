/*******************************************************************************
 * Copyright (c) 2015-2016 Open Analytics NV and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package eu.openanalytics.japyter.client;

import java.io.IOException;
import java.util.concurrent.atomic.AtomicBoolean;

public abstract class AbstractRunningChannel extends AbstractChannel
{
    private final AtomicBoolean running;

    public AbstractRunningChannel(final String address, final Session session, final boolean initiallyRunning)
    {
        super(address, session);

        running = new AtomicBoolean(initiallyRunning);
    }

    @Override
    public void close() throws IOException
    {
        if (isRunning())
        {
            stop();
        }

        super.close();
    }

    public boolean isRunning()
    {
        return running.get();
    }

    public void stop()
    {
        running.set(false);
    }

    public void start()
    {
        running.set(true);
    }
}
