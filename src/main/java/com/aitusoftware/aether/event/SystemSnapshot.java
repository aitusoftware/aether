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
package com.aitusoftware.aether.event;

import java.util.HashMap;
import java.util.HashSet;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aitusoftware.aether.annotation.CallerOwned;
import com.aitusoftware.aether.model.ChannelSessionKey;
import com.aitusoftware.aether.model.PublisherCounterSet;
import com.aitusoftware.aether.model.SubscriberCounterSet;
import com.aitusoftware.aether.model.SystemCounters;

/**
 * Maintains an aggregate view over multiple MediaDriver snapshots.
 */
public final class SystemSnapshot implements CounterSnapshotListener
{
    private final Map<StreamKey, Map<Integer, SubscriberCounterSet>> allSubscribers = new HashMap<>();
    private final Map<StreamKey, Map<ChannelSessionKey, Set<ChannelSessionKey>>> connectionsByStream = new HashMap<>();
    private final Map<ChannelSessionKey, PublisherCounterSet> publishersByRegistration = new HashMap<>();
    private final Map<ChannelSessionKey, SubscriberCounterSet> subscribersByRegistration = new HashMap<>();
    private final Map<String, SystemCounters> systemCountersByLabel = new HashMap<>();

    /**
     * {@inheritDoc}
     */
    @Override
    public void onSnapshot(
        final String label,
        final long timestamp,
        @CallerOwned final List<PublisherCounterSet> publisherCounters,
        @CallerOwned final List<SubscriberCounterSet> subscriberCounters,
        @CallerOwned final SystemCounters systemCounters)
    {
        systemCounters.copyInto(systemCountersByLabel.computeIfAbsent(label, key -> new SystemCounters()));
        for (final SubscriberCounterSet subscriberCounter : subscriberCounters)
        {
            final StreamKey streamKey = new StreamKey(
                subscriberCounter.channel().toString(), subscriberCounter.streamId());
            allSubscribers.computeIfAbsent(streamKey, key -> new HashMap<>())
                .put(subscriberCounter.sessionId(), subscriberCounter.copy());
            subscribersByRegistration.put(
                new ChannelSessionKey(label, subscriberCounter.channel().toString(), subscriberCounter.streamId(),
                subscriberCounter.sessionId()), subscriberCounter.copy());
        }

        for (final PublisherCounterSet publisherCounter : publisherCounters)
        {
            publishersByRegistration.put(
                new ChannelSessionKey(label, publisherCounter.channel().toString(), publisherCounter.streamId(),
                publisherCounter.sessionId()), publisherCounter.copy());

            final StreamKey publisherStreamKey = new StreamKey(
                publisherCounter.channel().toString(), publisherCounter.streamId());
            final Map<ChannelSessionKey, Set<ChannelSessionKey>> streamPublishers =
                connectionsByStream.computeIfAbsent(publisherStreamKey, key -> new HashMap<>());
            final ChannelSessionKey publisherChannelSessionKey = new ChannelSessionKey(
                label, publisherCounter.channel().toString(), publisherCounter.streamId(),
                publisherCounter.sessionId()
            );
            final Set<ChannelSessionKey> channelSessionKeys = streamPublishers
                .computeIfAbsent(publisherChannelSessionKey, key -> new HashSet<>());

            final Set<Map.Entry<ChannelSessionKey, SubscriberCounterSet>> subscribers =
                subscribersByRegistration.entrySet();
            for (final Map.Entry<ChannelSessionKey, SubscriberCounterSet> subscriber : subscribers)
            {
                if (
                    subscriber.getKey().getChannel().equals(publisherChannelSessionKey.getChannel()) &&
                    subscriber.getKey().getStreamId() == publisherChannelSessionKey.getStreamId() &&
                    subscriber.getKey().getSessionId() == publisherChannelSessionKey.getSessionId())
                {
                    channelSessionKeys.add(subscriber.getKey());
                }
            }
        }
    }

    /**
     * Returns system counters keyed by MediaDriver label.
     *
     * @return the system counters
     */
    public Map<String, SystemCounters> getSystemCounters()
    {
        return systemCountersByLabel;
    }

    /**
     * Returns a tree-like structure identifying participants in individual message flows.
     *
     * @return the structure of identifiers
     */
    public Map<StreamKey, Map<ChannelSessionKey, Set<ChannelSessionKey>>> getConnectionsByStream()
    {
        return connectionsByStream;
    }

    /**
     * Returns the publisher counters associated with a particular publisher.
     *
     * @param channelSessionKey the identifier of the publisher
     * @return the publisher counters
     */
    public PublisherCounterSet getPublisherCounterSet(final ChannelSessionKey channelSessionKey)
    {
        return publishersByRegistration.get(channelSessionKey);
    }

    /**
     * Returns the subscriber counters associated with a particular subscriber.
     *
     * @param channelSessionKey the identifier of the subscriber
     * @return the subscriber counters
     */
    public SubscriberCounterSet getSubscriberCounterSet(final ChannelSessionKey channelSessionKey)
    {
        return subscribersByRegistration.get(channelSessionKey);
    }
}