/*******************************************************************************
 * Copyright (c) 2015-2016 Open Analytics NV and others.
 * All rights reserved. This program and the accompanying materials
 * are made available under the terms of the Eclipse Public License v1.0
 * which accompanies this distribution, and is available at
 * http://www.eclipse.org/legal/epl-v10.html
 *******************************************************************************/

package eu.openanalytics.japyter.model;

import static java.util.UUID.randomUUID;
import static org.apache.commons.lang3.ArrayUtils.EMPTY_BYTE_ARRAY;
import static org.apache.commons.lang3.Validate.notNull;
import static org.apache.commons.lang3.builder.EqualsBuilder.reflectionEquals;
import static org.apache.commons.lang3.builder.HashCodeBuilder.reflectionHashCode;
import static org.apache.commons.lang3.builder.ToStringBuilder.reflectionToString;

import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

import eu.openanalytics.japyter.Japyter;
import eu.openanalytics.japyter.client.Protocol.RequestMessageType;
import eu.openanalytics.japyter.model.gen.Header;
import eu.openanalytics.japyter.model.gen.Reply;
import eu.openanalytics.japyter.model.gen.Request;

public class Message
{
    private final List<byte[]> zmqIdentities, extraData;
    private byte[] hmacSignature;
    private Map<String, Object> metadata, content;
    private Header header, parentHeader;

    /**
     * Creates a new empty message.
     */
    public Message()
    {
        zmqIdentities = new ArrayList<>();
        hmacSignature = EMPTY_BYTE_ARRAY;
        header = new Header();
        parentHeader = new Header();
        metadata = new HashMap<>();
        content = new HashMap<>();
        extraData = new ArrayList<>();
    }

    /**
     * Creates a new empty message with a random ID and the specified message type.
     */
    public Message(final RequestMessageType type)
    {
        this();

        getHeader().withMsgType(notNull(type).toString()).withMsgId(randomId());
    }

    /**
     * Creates a reply for the current message.
     */
    public Message createReply(final RequestMessageType type)
    {
        final Message reply = new Message().withParentHeader(getHeader());

        reply.getHeader().withMsgType(notNull(type.getReplyMessageType()).toString()).withMsgId(randomId());

        return reply;
    }

    private String randomId()
    {
        return Japyter.class.getSimpleName().toLowerCase() + "." + randomUUID();
    }

    public Message withZmqIdentity(final byte[] zmqIdentity)
    {
        zmqIdentities.add(notNull(zmqIdentity, "zmqIdentity can't be null"));
        return this;
    }

    public Message withHmacSignature(final byte[] hmacSignature)
    {
        this.hmacSignature = notNull(hmacSignature, "hmacSignature can't be null");
        return this;
    }

    public Message withHeader(final Header header)
    {
        this.header = notNull(header, "header can't be null");
        return this;
    }

    public Message withParentHeader(final Header parentHeader)
    {
        this.parentHeader = notNull(parentHeader, "parentHeader can't be null");
        return this;
    }

    public Message withMetadata(final Map<String, Object> metadata)
    {
        this.metadata = notNull(metadata, "metadata can't be null");
        return this;
    }

    public Message withContent(final Map<String, Object> content)
    {
        this.content = notNull(content, "content can't be null");
        return this;
    }

    @SuppressWarnings("unchecked")
    public Message withContent(final Request content)
    {
        this.content = Japyter.JSON_OBJECT_MAPPER.convertValue(notNull(content, "content can't be null"),
            Map.class);

        return this;
    }

    @SuppressWarnings("unchecked")
    public Message withContent(final Reply content)
    {
        this.content = Japyter.JSON_OBJECT_MAPPER.convertValue(notNull(content, "content can't be null"),
            Map.class);

        return this;
    }

    public Message withExtraDatum(final byte[] extraDatum)
    {
        extraData.add(notNull(extraDatum, "extraDatum can't be null"));
        return this;
    }

    public List<byte[]> getZmqIdentities()
    {
        return zmqIdentities;
    }

    public List<byte[]> getExtraData()
    {
        return extraData;
    }

    public byte[] getHmacSignature()
    {
        return hmacSignature;
    }

    public Header getHeader()
    {
        return header;
    }

    public Header getParentHeader()
    {
        return parentHeader;
    }

    public Map<String, Object> getMetadata()
    {
        return metadata;
    }

    public Map<String, Object> getContent()
    {
        return content;
    }

    @Override
    public String toString()
    {
        return reflectionToString(this);
    }

    @Override
    public boolean equals(final Object obj)
    {
        return reflectionEquals(this, obj);
    }

    @Override
    public int hashCode()
    {
        return reflectionHashCode(this);
    }
}
