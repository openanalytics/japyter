/*******************************************************************************
 * Copyright (c) 2015-2016 Open Analytics NV and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package eu.openanalytics.japyter.client;

import static eu.openanalytics.japyter.Japyter.JSON_OBJECT_MAPPER;
import static org.apache.commons.lang3.BooleanUtils.isTrue;

import java.io.IOException;

import org.apache.commons.lang3.Validate;
import org.zeromq.ZMQ;

import eu.openanalytics.japyter.client.Protocol.RequestMessageType;
import eu.openanalytics.japyter.model.Message;
import eu.openanalytics.japyter.model.gen.InputReply;
import eu.openanalytics.japyter.model.gen.InputRequest;
import eu.openanalytics.japyter.model.gen.Reply;
import eu.openanalytics.japyter.model.gen.Request;

public class Stdin extends AbstractRunningChannel
{
    private class StdinPoller implements Runnable
    {
        @Override
        public void run()
        {
            while (isRunning())
            {
                try
                {
                    pollAndReply();
                }
                catch (final Throwable t)
                {
                    if (isRunning())
                    {
                        getLogger().error("Polling and replying to stdin handler failed", t);
                    }
                }
            }

            getLogger().info("Stopped");
        }
    }

    public interface StdinHandler
    {
        String prompt(String text, boolean password);

        Reply other(Request request);
    }

    /**
     * Abstract implementation of {@link StdinHandler} that rejects "other" messages.
     */
    public abstract static class AbstractStdinHandler implements StdinHandler
    {
        @Override
        public Reply other(final Request request)
        {
            throw new UnsupportedOperationException("Unsupported Stdin request: " + request);
        }
    }

    private final StdinHandler handler;

    public Stdin(final String address, final Session session, final StdinHandler handler)
    {
        super(address, session, true);

        this.handler = Validate.notNull(handler, "handler can't be null");

        getSession().execute(new StdinPoller());
    }

    @Override
    protected int getZmqSocketType()
    {
        return ZMQ.DEALER;
    }

    private void pollAndReply() throws IOException
    {
        final Message maybeMessage = getSession().poll(getZmqSocket());

        if (maybeMessage == null)
        {
            return;
        }

        reply(maybeMessage);
    }

    private void reply(final Message message) throws IOException
    {
        final RequestMessageType type = RequestMessageType.fromValue(message.getHeader().getMsgType());

        if (getLogger().isDebugEnabled())
        {
            getLogger().debug("Selected type: {} for message: {}", type, message);
        }

        final Request request = JSON_OBJECT_MAPPER.convertValue(message.getContent(),
            type.getRequestContentClass());

        getSession().send(message.createReply(type).withContent(buildContent(request)), getZmqSocket());
    }

    private Reply buildContent(final Request request)
    {
        if (request instanceof InputRequest)
        {
            final InputRequest ir = (InputRequest) request;

            return new InputReply().withValue(handler.prompt(ir.getPrompt(), isTrue(ir.getPassword())));
        }
        else
        {
            return handler.other(request);
        }
    }
}
