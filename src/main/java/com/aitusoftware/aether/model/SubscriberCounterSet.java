/*
 * Copyright 2019 Aitu Software Limited.
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

import org.agrona.collections.Long2LongHashMap;

/**
 * Models counters associated with a subscriber.
 */
public final class SubscriberCounterSet implements SessionKeyed
{
    private final StringBuilder channel = new StringBuilder();
    private long receiverPosition;
    private long receiverHighWaterMark;
    private int sessionId;
    private int streamId;
    private Long2LongHashMap subscriberPositions = new Long2LongHashMap(Long.MIN_VALUE);

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
        subscriberPositions.clear();
    }

    public int subscriberCount()
    {
        return subscriberPositions.size();
    }

    public Long2LongHashMap subscriberPositions()
    {
        return subscriberPositions;
    }

    public void subscriberPosition(final long registrationId, final long subscriberPosition)
    {
        subscriberPositions.put(registrationId, subscriberPosition);
    }

    public void receiverPosition(final long receiverPosition)
    {
        this.receiverPosition = receiverPosition;
    }

    public void receiverHighWaterMark(final long receiverHighWaterMark)
    {
        this.receiverHighWaterMark = receiverHighWaterMark;
    }

    public void sessionId(final int sessionId)
    {
        this.sessionId = sessionId;
    }

    public long receiverPosition()
    {
        return receiverPosition;
    }

    public long receiverHighWaterMark()
    {
        return receiverHighWaterMark;
    }

    public int sessionId()
    {
        return sessionId;
    }

    public long incompleteData()
    {
        return Math.max(0, receiverHighWaterMark - receiverPosition);
    }

    public long inflight(final PublisherCounterSet publisher)
    {
        return Math.max(0, receiverHighWaterMark - publisher.senderPosition());
    }

    public CharSequence channel()
    {
        return channel;
    }

    public int streamId()
    {
        return streamId;
    }

    public SubscriberCounterSet copy()
    {
        final SubscriberCounterSet copy = new SubscriberCounterSet();
        copy.reset(channel(), sessionId(), streamId());
        copy.receiverHighWaterMark(receiverHighWaterMark());
        copy.receiverPosition(receiverPosition());

        final Long2LongHashMap.KeyIterator registrationIds = subscriberPositions.keySet().iterator();
        while (registrationIds.hasNext())
        {
            final long registrationId = registrationIds.nextValue();
            copy.subscriberPosition(registrationId, subscriberPositions.get(registrationId));
        }
        return copy;
    }
}
