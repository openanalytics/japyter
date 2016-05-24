/*******************************************************************************
 * Copyright (c) 2015-2016 Open Analytics NV and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package eu.openanalytics.japyter.client;

import static eu.openanalytics.japyter.client.Protocol.RequestMessageType.CONNECT_REQUEST;
import static eu.openanalytics.japyter.client.Protocol.RequestMessageType.KERNEL_INFO_REQUEST;
import static org.apache.commons.lang3.Validate.notBlank;
import static org.apache.commons.lang3.Validate.notNull;

import java.io.IOException;

import org.zeromq.ZMQ;

import eu.openanalytics.japyter.model.gen.CompleteReply;
import eu.openanalytics.japyter.model.gen.CompleteRequest;
import eu.openanalytics.japyter.model.gen.ConnectReply;
import eu.openanalytics.japyter.model.gen.ExecuteReply;
import eu.openanalytics.japyter.model.gen.ExecuteRequest;
import eu.openanalytics.japyter.model.gen.HistoryReply;
import eu.openanalytics.japyter.model.gen.HistoryRequest;
import eu.openanalytics.japyter.model.gen.InspectReply;
import eu.openanalytics.japyter.model.gen.InspectRequest;
import eu.openanalytics.japyter.model.gen.IsCompleteReply;
import eu.openanalytics.japyter.model.gen.IsCompleteRequest;
import eu.openanalytics.japyter.model.gen.KernelInfoReply;
import eu.openanalytics.japyter.model.gen.ShutdownReply;
import eu.openanalytics.japyter.model.gen.ShutdownRequest;

public class Shell extends AbstractSynchronousChannel
{
    public enum InspectDetailLevel
    {
        COARSE(0), FINE(1);

        private final int value;

        private InspectDetailLevel(final int value)
        {
            this.value = value;
        }

        public int getValue()
        {
            return value;
        }
    };

    public Shell(final String address, final Session session)
    {
        super(address, session);
    }

    @Override
    protected int getZmqSocketType()
    {
        return ZMQ.DEALER;
    }

    public ExecuteReply execute(final ExecuteRequest request) throws IOException
    {
        return send(request);
    }

    public InspectReply inspect(final String code, final int cursorPosition, final InspectDetailLevel level)
        throws IOException
    {
        return send(new InspectRequest().withCode(notBlank(code, "code can't be blank"))
            .withCursorPos(cursorPosition)
            .withDetailLevel(notNull(level, "level can't be null").getValue()));
    }

    public CompleteReply complete(final String code, final int cursorPosition) throws IOException
    {
        return send(new CompleteRequest().withCode(notBlank(code, "code can't be blank")).withCursorPos(
            cursorPosition));
    }

    public IsCompleteReply isComplete(final String code) throws IOException
    {
        return send(new IsCompleteRequest().withCode(notBlank(code, "code can't be blank")));
    }

    public HistoryReply history(final HistoryRequest request) throws IOException
    {
        return send(request);
    }

    public ConnectReply connect() throws IOException
    {
        return send(CONNECT_REQUEST);
    }

    public KernelInfoReply kernelInfo() throws IOException
    {
        return send(KERNEL_INFO_REQUEST);
    }

    public ShutdownReply shutdown(final boolean restart) throws IOException
    {
        return send(new ShutdownRequest().withRestart(restart));
    }
}
