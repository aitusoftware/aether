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

import com.aitusoftware.aether.aggregation.RateBucket;
import com.aitusoftware.aether.aggregation.StreamRate;
import com.aitusoftware.aether.model.ChannelSessionKey;
import com.aitusoftware.aether.model.PublisherCounterSet;
import com.aitusoftware.aether.model.SystemCounters;
import org.junit.jupiter.api.Test;

import java.util.*;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;

class RateMonitorTest
{
    private static final int SESSION_ID = 7;
    private static final int STREAM_ID = 11;
    private static final String CTX_0 = "first";
    private static final String CTX_1 = "second";
    private final RateMonitor rateMonitor = new RateMonitor(Arrays.asList(
        new RateBucket(10, TimeUnit.SECONDS),
        new RateBucket(30, TimeUnit.SECONDS)));

    @Test
    void shouldUpdateAndStoreStreamRatesByPublisher()
    {
        for (int i = 0; i < 30; i++)
        {
            rateMonitor.onSnapshot(CTX_0, TimeUnit.SECONDS.toMillis(100 + i),
                publishers(CTX_0, 1000L + 1000 * i, 2000L + 2000 * i),
                Collections.emptyList(), new SystemCounters());
            rateMonitor.onSnapshot(CTX_1, TimeUnit.SECONDS.toMillis(100 + i),
                publishers(CTX_1, 1000L + 3000 * i, 2000L + 5000 * i),
                Collections.emptyList(), new SystemCounters());
        }

        final Map<ChannelSessionKey, StreamRate> publisherRates = rateMonitor.publisherRates();

        assertPublisherRates(publisherRates, 1000L, 1000L, 0, CTX_0);
        assertPublisherRates(publisherRates, 2000L, 2000L, 1, CTX_0);
        assertPublisherRates(publisherRates, 3000L, 3000L, 0, CTX_1);
        assertPublisherRates(publisherRates, 5000L, 5000L, 1, CTX_1);
    }

    private void assertPublisherRates(
        final Map<ChannelSessionKey, StreamRate> publisherRates,
        final long firstRate, final long secondRate, final int index, final String context)
    {
        final List<Long> rates = new ArrayList<>();
        final StreamRate streamRate = publisherRates.get(
            new ChannelSessionKey(context, context + "_" + index, STREAM_ID, SESSION_ID));
        streamRate.consumeRates((duration, durationUnit, bytesPerSecond) -> rates.add(bytesPerSecond));
        assertThat(rates.size()).isEqualTo(2);

        assertThat(rates.get(0)).isEqualTo(firstRate);
        assertThat(rates.get(1)).isEqualTo(secondRate);
    }

    private List<PublisherCounterSet> publishers(final String label, final Long... streamPositions)
    {
        final List<PublisherCounterSet> publisherCounters = new ArrayList<>();
        for (int i = 0; i < streamPositions.length; i++)
        {
            final Long position = streamPositions[i];
            final PublisherCounterSet counterSet = new PublisherCounterSet();
            counterSet.reset(label + "_" + i, SESSION_ID, STREAM_ID);
            counterSet.publisherPosition(position);
            publisherCounters.add(counterSet);
        }
        return publisherCounters;
    }
}