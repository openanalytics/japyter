/*******************************************************************************
 * Copyright (c) 2015-2016 Open Analytics NV and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package eu.openanalytics.japyter.client;

import static com.fasterxml.jackson.core.JsonEncoding.UTF8;
import static eu.openanalytics.japyter.Japyter.JSON_OBJECT_MAPPER;
import static java.nio.charset.Charset.defaultCharset;
import static java.util.Arrays.asList;
import static org.apache.commons.codec.binary.Hex.encodeHexString;
import static org.apache.commons.codec.digest.HmacUtils.getInitializedMac;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.apache.commons.lang3.StringUtils.equalsIgnoreCase;
import static org.apache.commons.lang3.StringUtils.isBlank;
import static org.apache.commons.lang3.StringUtils.isNotBlank;
import static org.apache.commons.lang3.Validate.notBlank;
import static org.apache.commons.lang3.Validate.notNull;

import java.io.IOException;
import java.nio.charset.Charset;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.HashMap;
import java.util.Iterator;
import java.util.List;
import java.util.Map;

import javax.crypto.Mac;

import org.apache.commons.codec.digest.HmacAlgorithms;
import org.apache.commons.lang3.ArrayUtils;
import org.apache.commons.lang3.StringUtils;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;

import eu.openanalytics.japyter.model.Message;
import eu.openanalytics.japyter.model.gen.Broadcast;
import eu.openanalytics.japyter.model.gen.ClearOutput;
import eu.openanalytics.japyter.model.gen.CompleteReply;
import eu.openanalytics.japyter.model.gen.CompleteRequest;
import eu.openanalytics.japyter.model.gen.ConnectReply;
import eu.openanalytics.japyter.model.gen.DataPub;
import eu.openanalytics.japyter.model.gen.DisplayData;
import eu.openanalytics.japyter.model.gen.Error;
import eu.openanalytics.japyter.model.gen.ExecuteInput;
import eu.openanalytics.japyter.model.gen.ExecuteReply;
import eu.openanalytics.japyter.model.gen.ExecuteRequest;
import eu.openanalytics.japyter.model.gen.ExecuteResult;
import eu.openanalytics.japyter.model.gen.Header;
import eu.openanalytics.japyter.model.gen.HistoryReply;
import eu.openanalytics.japyter.model.gen.HistoryRequest;
import eu.openanalytics.japyter.model.gen.InputReply;
import eu.openanalytics.japyter.model.gen.InputRequest;
import eu.openanalytics.japyter.model.gen.InspectReply;
import eu.openanalytics.japyter.model.gen.InspectRequest;
import eu.openanalytics.japyter.model.gen.IsCompleteReply;
import eu.openanalytics.japyter.model.gen.IsCompleteRequest;
import eu.openanalytics.japyter.model.gen.KernelInfoReply;
import eu.openanalytics.japyter.model.gen.Reply;
import eu.openanalytics.japyter.model.gen.Request;
import eu.openanalytics.japyter.model.gen.ShutdownReply;
import eu.openanalytics.japyter.model.gen.ShutdownRequest;
import eu.openanalytics.japyter.model.gen.Status;
import eu.openanalytics.japyter.model.gen.Stream;

public class Protocol
{
    /**
     * List of known message types from
     * http://ipython.org/ipython-doc/stable/development/messaging.html
     */

    public static enum RequestMessageType
    {
        EXECUTE_REQUEST("execute_request", ExecuteRequest.class, ExecuteReply.class,
                        ReplyMessageType.EXECUTE_REPLY), INSPECT_REQUEST("inspect_request",
                        InspectRequest.class, InspectReply.class, ReplyMessageType.INSPECT_REPLY), COMPLETE_REQUEST(
                        "complete_request", CompleteRequest.class, CompleteReply.class,
                        ReplyMessageType.COMPLETE_REPLY), HISTORY_REQUEST("history_request",
                        HistoryRequest.class, HistoryReply.class, ReplyMessageType.HISTORY_REPLY), IS_COMPLETE_REQUEST(
                        "is_complete_request", IsCompleteRequest.class, IsCompleteReply.class,
                        ReplyMessageType.IS_COMPLETE_REPLY), CONNECT_REQUEST("connect_request", null,
                        ConnectReply.class, ReplyMessageType.CONNECT_REPLY), KERNEL_INFO_REQUEST(
                        "kernel_info_request", null, KernelInfoReply.class,
                        ReplyMessageType.KERNEL_INFO_REPLY), SHUTDOWN_REQUEST("shutdown_request",
                        ShutdownRequest.class, ShutdownReply.class, ReplyMessageType.SHUTDOWN_REPLY), INPUT_REQUEST(
                        "input_request", InputRequest.class, InputReply.class, ReplyMessageType.INPUT_REPLY), OTHER(
                        "*", Request.class, Reply.class, ReplyMessageType.OTHER);

        private final String value;
        private final Class<? extends Request> requestContentClass;
        private final Class<? extends Reply> replyContentClass;
        private final ReplyMessageType replyMessageType;

        private static Map<String, RequestMessageType> values = new HashMap<>();
        private static Map<Class<? extends Request>, RequestMessageType> requestContentClasses = new HashMap<>();

        static
        {
            for (final RequestMessageType c : values())
            {
                values.put(c.value, c);

                if (c.requestContentClass != null)
                {
                    requestContentClasses.put(c.requestContentClass, c);
                }
            }
        }

        private RequestMessageType(final String value,
                                   final Class<? extends Request> requestContentClass,
                                   final Class<? extends Reply> replyContentClass,
                                   final ReplyMessageType replyMessageType)
        {
            this.value = notBlank(value);
            this.requestContentClass = requestContentClass;
            this.replyContentClass = notNull(replyContentClass);
            this.replyMessageType = notNull(replyMessageType);
        }

        @Override
        public String toString()
        {
            return this.value;
        }

        public Class<? extends Request> getRequestContentClass()
        {
            return requestContentClass;
        }

        public Class<? extends Reply> getReplyContentClass()
        {
            return replyContentClass;
        }

        public ReplyMessageType getReplyMessageType()
        {
            return replyMessageType;
        }

        public static RequestMessageType fromRequestContentClass(final Class<? extends Request> requestContentClass)
        {
            final RequestMessageType rmt = requestContentClasses.get(requestContentClass);
            return rmt != null ? rmt : RequestMessageType.OTHER;
        }

        public static RequestMessageType fromValue(final String value)
        {
            final RequestMessageType rmt = values.get(value);
            return rmt != null ? rmt : RequestMessageType.OTHER;
        }
    }

    public static enum ReplyMessageType
    {
        EXECUTE_REPLY("execute_reply"), INSPECT_REPLY("inspect_reply"), COMPLETE_REPLY("complete_reply"), HISTORY_REPLY(
                        "history_reply"), IS_COMPLETE_REPLY("is_complete_reply"), CONNECT_REPLY(
                        "connect_reply"), KERNEL_INFO_REPLY("kernel_info_reply"), SHUTDOWN_REPLY(
                        "shutdown_reply"), INPUT_REPLY("input_reply"), OTHER("*");

        private final String value;

        private ReplyMessageType(final String value)
        {
            this.value = notBlank(value);
        }

        @Override
        public String toString()
        {
            return this.value;
        }
    }

    public static enum BroadcastType
    {
        STREAM("stream", Stream.class), DISPLAY_DATA("display_data", DisplayData.class), DATA_PUB("data_pub",
                        DataPub.class), EXECUTE_INPUT("execute_input", ExecuteInput.class), EXECUTE_RESULT(
                        "execute_result", ExecuteResult.class), ERROR("error", Error.class), STATUS("status",
                        Status.class), CLEAR_OUTPUT("clear_output", ClearOutput.class);

        private final String value;
        private final Class<? extends Broadcast> broadcastClass;

        private static Map<String, BroadcastType> values = new HashMap<>();

        static
        {
            for (final BroadcastType c : values())
            {
                values.put(c.value, c);
            }
        }

        private BroadcastType(final String value, final Class<? extends Broadcast> broadcastClass)
        {
            this.value = notBlank(value);
            this.broadcastClass = notNull(broadcastClass);
        }

        @Override
        public String toString()
        {
            return this.value;
        }

        public Class<? extends Broadcast> getBroadcastClass()
        {
            return broadcastClass;
        }

        public static Class<? extends Broadcast> classFromValue(final String value)
        {
            final BroadcastType broadcastType = values.get(value);

            // fallback to generic Broadcast class for unsupported types
            return broadcastType != null ? broadcastType.broadcastClass : Broadcast.class;
        }
    }

    public static enum CustomMessageType
    {
        COMM_OPEN("comm_open"), COMM_MSG("comm_msg"), COMM_CLOSE("comm_close");

        private final String value;

        private CustomMessageType(final String value)
        {
            this.value = notBlank(value);
        }

        @Override
        public String toString()
        {
            return this.value;
        }
    }

    public static final String VERSION = "5.0";
    public static final String DELIMITER = "<IDS|MSG>";
    public static final Charset ENCODING = Charset.forName(UTF8.getJavaName());

    private static final Logger LOGGER = LoggerFactory.getLogger(Protocol.class);
    private static final byte[] DELIMITER_BYTES = DELIMITER.getBytes(ENCODING);
    private static final byte[] NO_SIGNATURE_BYTES = EMPTY_BYTE_ARRAY;

    private final byte[] hmacKey;
    private final HmacAlgorithms hmacAlgorithm;

    public Protocol()
    {
        this(null, (HmacAlgorithms) null);
    }

    public Protocol(final byte[] hmacKey, final HmacAlgorithms hmacAlgorithm)
    {
        this.hmacAlgorithm = hmacAlgorithm;
        this.hmacKey = ArrayUtils.clone(hmacKey);

        if (hmacAlgorithm != null && hmacKey != null)
        {
            LOGGER.info("HMAC signature enabled with: {}", hmacAlgorithm);
        }
        else
        {

            LOGGER.info("HMAC signature is disabled");
        }
    }

    public Protocol(final String key, final String signatureScheme)
    {
        this(getKeyBytes(key), getHmacAlgorithm(signatureScheme));
    }

    private static byte[] getKeyBytes(final String key)
    {
        return isNotBlank(key) ? key.getBytes(defaultCharset()) : null;
    }

    private static HmacAlgorithms getHmacAlgorithm(final String signatureScheme)
    {
        if (isBlank(signatureScheme))
        {
            return null;
        }

        final String algo = StringUtils.remove(signatureScheme, '-');

        for (final HmacAlgorithms ha : HmacAlgorithms.values())
        {
            if (equalsIgnoreCase(ha.toString(), algo))
            {
                return ha;
            }
        }

        throw new IllegalArgumentException("Unsupported signature scheme: " + signatureScheme);
    }

    public boolean isSigning()
    {
        return hmacKey != null && hmacAlgorithm != null;
    }

    public byte[] getHmacKey()
    {
        return ArrayUtils.clone(hmacKey);
    }

    public HmacAlgorithms getHmacAlgorithm()
    {
        return hmacAlgorithm;
    }

    public List<byte[]> toFrames(final Message message) throws IOException
    {
        message.getHeader().setVersion(VERSION);

        final List<byte[]> jsonFrames = asList(
            // writeValueAsBytes uses UTF-8 encoding
            JSON_OBJECT_MAPPER.writeValueAsBytes(message.getHeader()),
            JSON_OBJECT_MAPPER.writeValueAsBytes(message.getParentHeader()),
            JSON_OBJECT_MAPPER.writeValueAsBytes(message.getMetadata()),
            JSON_OBJECT_MAPPER.writeValueAsBytes(message.getContent()));

        final List<byte[]> frames = new ArrayList<>();
        frames.addAll(message.getZmqIdentities());
        frames.add(DELIMITER_BYTES);
        frames.add(maybeSignature(jsonFrames));
        frames.addAll(jsonFrames);
        frames.addAll(message.getExtraData());
        return frames;
    }

    private byte[] maybeSignature(final List<byte[]> jsonFrames)
    {
        if (!isSigning())
        {
            return NO_SIGNATURE_BYTES;
        }

        final Mac mac = getInitializedMac(hmacAlgorithm, hmacKey);
        for (final byte[] jsonFrame : jsonFrames)
        {
            mac.update(jsonFrame);
        }

        return encodeHexString(mac.doFinal()).getBytes(ENCODING);
    }

    @SuppressWarnings("unchecked")
    private enum FrameHandler
    {
        ZMQ_ID
        {
            @Override
            boolean handle(final byte[] frame, final List<byte[]> jsonFrames, final Message message)
            {
                if (Arrays.equals(frame, DELIMITER_BYTES))
                {
                    return true;
                }
                else
                {
                    message.withZmqIdentity(frame);
                    return false;
                }
            }
        },
        HMAC_SIGNATURE
        {
            @Override
            boolean handle(final byte[] frame, final List<byte[]> jsonFrames, final Message message)
            {
                message.withHmacSignature(frame);
                return true;
            }
        },
        HEADER
        {
            @Override
            boolean handle(final byte[] frame, final List<byte[]> jsonFrames, final Message message)
                throws IOException
            {
                jsonFrames.add(frame);
                message.withHeader(JSON_OBJECT_MAPPER.readValue(frame, Header.class));
                return true;
            }
        },
        PARENT_HEADER
        {
            @Override
            boolean handle(final byte[] frame, final List<byte[]> jsonFrames, final Message message)
                throws IOException
            {
                jsonFrames.add(frame);
                message.withParentHeader(JSON_OBJECT_MAPPER.readValue(frame, Header.class));
                return true;
            }
        },
        METADATA
        {
            @Override
            boolean handle(final byte[] frame, final List<byte[]> jsonFrames, final Message message)
                throws IOException
            {
                jsonFrames.add(frame);
                message.withMetadata(JSON_OBJECT_MAPPER.readValue(frame, Map.class));
                return true;
            }
        },
        CONTENT
        {
            @Override
            boolean handle(final byte[] frame, final List<byte[]> jsonFrames, final Message message)
                throws IOException
            {
                jsonFrames.add(frame);
                message.withContent(JSON_OBJECT_MAPPER.readValue(frame, Map.class));
                return true;
            }
        },
        EXTRA_DATA
        {
            @Override
            boolean handle(final byte[] frame, final List<byte[]> jsonFrames, final Message message)
            {
                message.withExtraDatum(frame);
                return false;
            }
        };

        abstract boolean handle(byte[] frame, final List<byte[]> jsonFrames, Message message)
            throws IOException;
    };

    public Message fromFrames(final List<byte[]> frames) throws IOException
    {
        final Message message = new Message();
        final List<byte[]> jsonFrames = new ArrayList<>();

        final Iterator<FrameHandler> i = asList(FrameHandler.values()).iterator();
        FrameHandler frameHandler = i.next();

        for (final byte[] frame : frames)
        {
            if (LOGGER.isDebugEnabled())
            {
                LOGGER.debug("{} handling frame: {}", frameHandler, new String(frame, Protocol.ENCODING));
            }

            if (frameHandler.handle(frame, jsonFrames, message))
            {
                frameHandler = i.next();
            }
        }

        if (frameHandler != FrameHandler.EXTRA_DATA)
        {
            throw new IOException("Not enough frames received, last frame: " + frameHandler);
        }

        if (!Arrays.equals(message.getHmacSignature(), maybeSignature(jsonFrames)))
        {
            throw new IOException("Invalid HMAC signature in received message");
        }

        return message;
    }
}
