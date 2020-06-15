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

import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aitusoftware.aether.annotation.CallerOwned;
import com.aitusoftware.aether.model.ChannelSessionKey;
import com.aitusoftware.aether.model.PublisherCounterSet;
import com.aitusoftware.aether.model.SubscriberCounterSet;
import com.aitusoftware.aether.model.SystemCounters;

import org.agrona.collections.Long2LongHashMap;

public final class ConsolePrinter implements CounterSnapshotListener
{
    private final SystemSnapshot systemSnapshot = new SystemSnapshot();

    @Override
    public void onSnapshot(
        final String label,
        final long timestamp,
        @CallerOwned final List<PublisherCounterSet> publisherCounters,
        @CallerOwned final List<SubscriberCounterSet> subscriberCounters,
        @CallerOwned final SystemCounters systemCounters)
    {
        systemSnapshot.onSnapshot(label, timestamp, publisherCounters, subscriberCounters, systemCounters);
        systemSnapshot.getSystemCounters().forEach((contextLabel, counters) ->
        {
            System.out.printf("===== System counters for \"%s\" =====%n", contextLabel);
            System.out.printf("Bytes sent:      %20d%n", counters.bytesSent());
            System.out.printf("Bytes received:  %20d%n", counters.bytesReceived());
            System.out.printf("NAKs sent:       %20d%n", counters.naksSent());
            System.out.printf("NAKs received:   %20d%n", counters.naksReceived());
            System.out.printf("Errors:          %20d%n", counters.errors());
            System.out.printf("Client timeouts: %20d%n", counters.clientTimeouts());
        });
        final Map<StreamKey, Map<ChannelSessionKey, Set<ChannelSessionKey>>> connectionsByStream =
            systemSnapshot.getConnectionsByStream();
        System.out.printf("===== Monitoring %d channels =====%n", connectionsByStream.size());
        for (final StreamKey streamKey : connectionsByStream.keySet())
        {
            System.out.printf("==== %s/%d ====%n", streamKey.getChannel(), streamKey.getStreamId());
            final Set<Map.Entry<ChannelSessionKey, Set<ChannelSessionKey>>> sources =
                connectionsByStream.get(streamKey).entrySet();
            for (final Map.Entry<ChannelSessionKey, Set<ChannelSessionKey>> source : sources)
            {
                final ChannelSessionKey publisherKey = source.getKey();
                final PublisherCounterSet publisher = systemSnapshot.getPublisherCounterSet(publisherKey);
                System.out.printf("%n---- Publisher Session %d ----%n", publisher.sessionId());
                System.out.printf("| publisher position:  %20d%n", publisher.publisherPosition());
                System.out.printf("| publisher limit:     %20d%n", publisher.publisherLimit());
                System.out.printf("| sender position:     %20d%n", publisher.senderPosition());
                System.out.printf("| sender limit:        %20d%n", publisher.senderLimit());
                final Set<ChannelSessionKey> subscribers = source.getValue();
                for (final ChannelSessionKey subscriberKey : subscribers)
                {
                    final SubscriberCounterSet subscriber =
                        systemSnapshot.getSubscriberCounterSet(subscriberKey);
                    System.out.printf("---- Subscriber ----%n");
                    System.out.printf("| receiver position:   %20d%n", subscriber.receiverPosition());
                    System.out.printf("| receiver HWM:        %20d%n", subscriber.receiverHighWaterMark());
                    final Long2LongHashMap subscriberPositions = subscriber.subscriberPositions();
                    subscriberPositions.forEach((reg, pos) ->
                    {
                        System.out.printf("| position (%d):        %20d%n", reg, pos);
                    });
                }
                System.out.printf("-----------------------------------------%n");
            }
        }
    }
}