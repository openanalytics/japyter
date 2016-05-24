/*******************************************************************************
 * Copyright (c) 2015-2016 Open Analytics NV and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package eu.openanalytics.japyter.client;

import static eu.openanalytics.japyter.Japyter.JSON_OBJECT_MAPPER;

import java.io.IOException;

import eu.openanalytics.japyter.client.Protocol.RequestMessageType;
import eu.openanalytics.japyter.model.Message;
import eu.openanalytics.japyter.model.gen.Reply;
import eu.openanalytics.japyter.model.gen.Request;

public abstract class AbstractSynchronousChannel extends AbstractChannel
{
    public AbstractSynchronousChannel(final String address, final Session session)
    {
        super(address, session);
    }

    public Message send(final Message message) throws IOException
    {
        getSession().send(message, getZmqSocket());

        getLogger().info("Sent message ID: {}", message.getHeader().getMsgId());

        return getSession().receive(getZmqSocket());
    }

    @SuppressWarnings("unchecked")
    protected <T extends Reply> T send(final RequestMessageType type, final Request content) throws IOException
    {
        final Message request = new Message(type);
        if (content != null)
        {
            request.withContent(content);
        }

        final Message reply = send(request);

        final Reply replyValue = JSON_OBJECT_MAPPER.convertValue(reply.getContent(),
            type.getReplyContentClass());

        return (T) replyValue;
    }

    protected <T extends Reply> T send(final RequestMessageType type) throws IOException
    {
        return send(type, null);
    }

    protected <T extends Reply> T send(final Request content) throws IOException
    {
        return send(RequestMessageType.fromRequestContentClass(content.getClass()), content);
    }
}
