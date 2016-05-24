/*******************************************************************************
 * Copyright (c) 2015-2016 Open Analytics NV and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package eu.openanalytics.japyter.client;

import static eu.openanalytics.japyter.client.Protocol.ENCODING;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.hamcrest.Matchers.is;
import static org.hamcrest.Matchers.isEmptyOrNullString;
import static org.hamcrest.Matchers.not;
import static org.hamcrest.Matchers.notNullValue;
import static org.junit.Assert.assertThat;

import java.io.IOException;
import java.util.List;

import org.apache.commons.lang3.ArrayUtils;
import org.junit.Test;

import eu.openanalytics.japyter.model.Message;

public class ProtocolTestCase
{
    private static final String[] DEFAULT_EXPECTED_FRAMES =
    {
        "zmq-id", "<IDS|MSG>", "<HMAC>",
        "{\"msg_id\":\"test-message-id\",\"msg_type\":\"connect_request\",\"version\":\"5.0\"}",
        "{\"msg_id\":\"test-parent-message-id\"}", "{\"meta-key\":\"meta-value\"}",
        "{\"content-key\":\"content-value\"}", "extra-data"
    };

    @Test
    public void noHmacSignature() throws IOException
    {
        testProtocol(new Protocol());
    }

    @Test
    public void validHmacSignatureScheme() throws IOException
    {
        testProtocol(new Protocol("f32beb57-4bc5-4cd0-8689-aa90b774ddc0", "hmac-sha256"));
    }

    @Test(expected = IllegalArgumentException.class)
    public void invalidHmacSignatureScheme()
    {
        new Protocol("f32beb57-4bc5-4cd0-8689-aa90b774ddc0", "hmac-sha1024");
    }

    private void testProtocol(final Protocol protocol) throws IOException
    {
        for (byte config = 0; config < 32; config++)
        {
            testProtocol(protocol, newTestMessage(config));
        }
    }

    private Message newTestMessage(final byte config)
    {
        final Message message = new Message();

        message.getHeader()
            .withMsgType(Protocol.RequestMessageType.CONNECT_REQUEST.toString())
            .withMsgId("test-message-id");

        if ((config & 1) != 0)
        {
            message.getZmqIdentities().add("zmq-id".getBytes(ENCODING));
        }

        if ((config & 2) != 0)
        {
            message.getParentHeader().withMsgId("test-parent-message-id");
        }

        if ((config & 4) != 0)
        {
            message.getMetadata().put("meta-key", "meta-value");
        }

        if ((config & 8) != 0)
        {
            message.getContent().put("content-key", "content-value");
        }

        if ((config & 16) != 0)
        {
            message.getExtraData().add("extra-data".getBytes(ENCODING));
        }

        return message;
    }

    private void testProtocol(final Protocol protocol, final Message message) throws IOException
    {
        final List<byte[]> frames = protocol.toFrames(message);

        final String[] expectedFrames = ArrayUtils.clone(DEFAULT_EXPECTED_FRAMES);
        if (!protocol.isSigning())
        {
            expectedFrames[2] = "";
        }
        if (message.getParentHeader().getMsgId() == null)
        {
            expectedFrames[4] = "{}";
        }
        if (message.getMetadata().isEmpty())
        {
            expectedFrames[5] = "{}";
        }
        if (message.getContent().isEmpty())
        {
            expectedFrames[6] = "{}";
        }

        int expectedFrameIndex = message.getZmqIdentities().isEmpty() ? 1 : 0;

        for (final byte[] frame : frames)
        {
            if (expectedFrameIndex == 2)
            {
                if (protocol.isSigning())
                {
                    assertThat(new String(frame, ENCODING), not(isEmptyOrNullString()));
                }
                else
                {
                    assertThat(frame, is(EMPTY_BYTE_ARRAY));
                }
            }
            else
            {
                assertThat(new String(frame, ENCODING), is(expectedFrames[expectedFrameIndex]));
            }

            expectedFrameIndex++;
        }

        final Message roundtripedMessage = protocol.fromFrames(frames);

        if (protocol.isSigning())
        {
            assertThat(roundtripedMessage.getHmacSignature(), is(notNullValue()));
            // the original message didn't have a signature -> set it before compare
            assertThat(roundtripedMessage,
                is(message.withHmacSignature(roundtripedMessage.getHmacSignature())));
        }
        else
        {
            assertThat(roundtripedMessage.getHmacSignature(), is(EMPTY_BYTE_ARRAY));
            // the original message didn't have a signature -> set it before compare
            assertThat(roundtripedMessage, is(message));
        }
    }
}
