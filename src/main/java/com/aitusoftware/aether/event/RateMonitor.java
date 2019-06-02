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
package com.aitusoftware.aether.event;

import com.aitusoftware.aether.aggregation.RateBucket;
import com.aitusoftware.aether.aggregation.StreamRate;
import com.aitusoftware.aether.annotation.CallerOwned;
import com.aitusoftware.aether.model.ChannelSessionKey;
import com.aitusoftware.aether.model.PublisherCounterSet;
import com.aitusoftware.aether.model.SubscriberCounterSet;
import com.aitusoftware.aether.model.SystemCounters;

import java.util.HashMap;
import java.util.List;
import java.util.Map;

public final class RateMonitor implements CounterSnapshotListener
{
    private final List<RateBucket> rateBuckets;
    private final HashMap<ChannelSessionKey, StreamRate> streamRateByPublisher = new HashMap<>();

    public RateMonitor(final List<RateBucket> rateBuckets)
    {
        this.rateBuckets = rateBuckets;
    }

    @Override
    public void onSnapshot(
        final String label,
        final long timestamp,
        @CallerOwned final List<PublisherCounterSet> publisherCounters,
        @CallerOwned final List<SubscriberCounterSet> subscriberCounters,
        @CallerOwned final SystemCounters systemCounters)
    {
        for (int i = 0; i < publisherCounters.size(); i++)
        {
            final PublisherCounterSet publisherCounter = publisherCounters.get(i);
            final ChannelSessionKey streamKey = new ChannelSessionKey(label, publisherCounter.channel().toString(),
                publisherCounter.streamId(), publisherCounter.sessionId());
            StreamRate streamRate = streamRateByPublisher.get(streamKey);
            if (streamRate == null)
            {
                streamRate = new StreamRate(rateBuckets);
                streamRateByPublisher.put(streamKey, streamRate);
            }

            streamRate.streamPosition(timestamp, publisherCounter.publisherPosition());
        }
    }

    public Map<ChannelSessionKey, StreamRate> publisherRates()
    {
        return streamRateByPublisher;
    }
}