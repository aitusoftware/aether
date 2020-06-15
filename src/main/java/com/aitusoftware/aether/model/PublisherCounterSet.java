/*
 * Copyright 2019-2020 Aitu Software Limited.
 *
 * https://aitusoftware.com
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.aitusoftware.aether.model;

/**
 * Models counters associated with a publisher.
 */
public final class PublisherCounterSet implements SessionKeyed
{
    private final StringBuilder channel = new StringBuilder();
    private long publisherPosition;
    private long backPressureEvents;
    private long senderPosition;
    private long senderLimit;
    private long publisherLimit;
    private int sessionId;
    private int streamId;

    /**
     * {@inheritDoc}
     */
    @Override
    public void reset(final CharSequence channel, final int sessionId, final int streamId)
    {
        this.channel.setLength(0);
        this.channel.append(channel);
        this.streamId = streamId;
        this.sessionId = sessionId;
    }

    public void publisherPosition(final long publisherPosition)
    {
        this.publisherPosition = publisherPosition;
    }

    public void backPressureEvents(final long backPressureEvents)
    {
        this.backPressureEvents = backPressureEvents;
    }

    public void senderPosition(final long senderPosition)
    {
        this.senderPosition = senderPosition;
    }

    public void senderLimit(final long senderLimit)
    {
        this.senderLimit = senderLimit;
    }

    public void publisherLimit(final long publisherLimit)
    {
        this.publisherLimit = publisherLimit;
    }

    public void sessionId(final int sessionId)
    {
        this.sessionId = sessionId;
    }

    public long publisherPosition()
    {
        return publisherPosition;
    }

    public long backPressureEvents()
    {
        return backPressureEvents;
    }

    public long senderPosition()
    {
        return senderPosition;
    }

    public long senderLimit()
    {
        return senderLimit;
    }

    public long publisherLimit()
    {
        return publisherLimit;
    }

    public int sessionId()
    {
        return sessionId;
    }

    public long publisherBufferRemaining()
    {
        return publisherLimit - publisherPosition;
    }

    public long buffered()
    {
        return Math.max(0, publisherPosition - senderPosition);
    }

    public CharSequence channel()
    {
        return channel;
    }

    public int streamId()
    {
        return streamId;
    }

    public PublisherCounterSet copy()
    {
        final PublisherCounterSet copy = new PublisherCounterSet();
        copy.reset(channel().toString(), sessionId(), streamId());
        copy.backPressureEvents(backPressureEvents());
        copy.publisherLimit(publisherLimit());
        copy.publisherPosition(publisherPosition());
        copy.senderLimit(senderLimit());
        copy.senderPosition(senderPosition());
        return copy;
    }
}