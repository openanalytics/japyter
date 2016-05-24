/*******************************************************************************
 * Copyright (c) 2015-2016 Open Analytics NV and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package eu.openanalytics.japyter.client;

import static org.apache.commons.lang3.Validate.notBlank;
import static org.apache.commons.lang3.Validate.notNull;

import java.io.Closeable;
import java.io.IOException;

import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.zeromq.ZMQ.Socket;

public abstract class AbstractChannel implements Closeable
{
    private final Logger logger = LoggerFactory.getLogger(getClass());

    private final String address;
    private final Session session;
    private final Socket zmqSocket;

    public AbstractChannel(final String address, final Session session)
    {
        this.address = notBlank(address, "address can't be null");
        this.session = notNull(session, "session can't be null");

        zmqSocket = session.connect(this);
    }

    @Override
    public void close() throws IOException
    {
        session.disconnect(this);
    }

    protected Logger getLogger()
    {
        return logger;
    }

    protected String getAddress()
    {
        return address;
    }

    protected Socket getZmqSocket()
    {
        return zmqSocket;
    }

    protected Session getSession()
    {
        return session;
    }

    protected abstract int getZmqSocketType();
}
