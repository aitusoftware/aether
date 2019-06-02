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
package com.aitusoftware.aether.aggregation;

import org.agrona.collections.Long2ObjectHashMap;

import java.util.ArrayList;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static java.util.Comparator.comparingLong;

public final class StreamRate
{
    @SuppressWarnings("unchecked")
    private final Long2ObjectHashMap<RollingWindow>[] rollingWindowsByTimeUnit =
        new Long2ObjectHashMap[TimeUnit.values().length];

    public StreamRate(final List<RateBucket> rateBuckets)
    {
        final List<RateBucket> copy = new ArrayList<>(rateBuckets);
        copy.sort(comparingLong(bucket -> bucket.getDurationUnit().toNanos(bucket.getDuration())));

        for (final RateBucket rateBucket : copy)
        {
            if (rollingWindowsByTimeUnit[rateBucket.getDurationUnit().ordinal()] == null)
            {
                rollingWindowsByTimeUnit[rateBucket.getDurationUnit().ordinal()] = new Long2ObjectHashMap<>();
            }

            final Long2ObjectHashMap<RollingWindow> rollingWindows =
                rollingWindowsByTimeUnit[rateBucket.getDurationUnit().ordinal()];
            if (rollingWindows.containsKey(rateBucket.getDuration()))
            {
                throw new IllegalArgumentException("Bucket already defined: " +
                    rateBucket.getDuration() + " " + rateBucket.getDurationUnit());
            }

            rollingWindows.put(rateBucket.getDuration(),
                new RollingWindow((int)rateBucket.getDurationUnit().toSeconds(rateBucket.getDuration()),
                rateBucket.getDurationUnit()));
        }
    }

    public void streamPosition(final long epochMillis, final long position)
    {
        for (int i = 0; i < rollingWindowsByTimeUnit.length; i++)
        {
            final Long2ObjectHashMap<RollingWindow> rollingWindowsByDuration = rollingWindowsByTimeUnit[i];
            if (rollingWindowsByDuration != null)
            {
                final Long2ObjectHashMap<RollingWindow>.KeyIterator keys = rollingWindowsByDuration.keySet().iterator();
                while (keys.hasNext())
                {
                    final long duration = keys.nextLong();
                    rollingWindowsByDuration.get(duration).updateBytePosition(epochMillis, position);
                }
            }
        }
    }

    public void consumeRates(final RateConsumer rateConsumer)
    {
        for (int i = 0; i < rollingWindowsByTimeUnit.length; i++)
        {
            final Long2ObjectHashMap<RollingWindow> rollingWindowsByDuration = rollingWindowsByTimeUnit[i];
            if (rollingWindowsByDuration != null)
            {
                final Long2ObjectHashMap<RollingWindow>.KeyIterator keys = rollingWindowsByDuration.keySet().iterator();
                while (keys.hasNext())
                {
                    final long duration = keys.nextLong();
                    final RollingWindow rollingWindow = rollingWindowsByDuration.get(duration);
                    final long averageValue = rollingWindow.getAverageValue();
                    rateConsumer.onAggregateRate(duration, rollingWindow.durationUnit, averageValue);
                }
            }
        }
    }

    private static final class RollingWindow
    {
        private final long[] segmentValues;
        private final TimeUnit durationUnit;
        private long lastUpdate;
        private int pointer = 0;

        RollingWindow(final int numberOfSegments, final TimeUnit durationUnit)
        {
            segmentValues = new long[numberOfSegments];
            this.durationUnit = durationUnit;
        }

        void updateBytePosition(final long epochMillis, final long bytePosition)
        {
            if (epochMillis >= lastUpdate + TimeUnit.SECONDS.toMillis(1))
            {
                segmentValues[pointer % segmentValues.length] = bytePosition;
                pointer++;
                lastUpdate = epochMillis;
            }
        }

        long getAverageValue()
        {
            final int endPointer = pointer - 1;
            final int startPointer = Math.max(0, pointer - segmentValues.length);

            long accumulator = 0L;
            int count = 0;
            for (int i = startPointer; i < endPointer; i++)
            {
                final long segmentDelta = segmentValues[(i + 1) % segmentValues.length] -
                    segmentValues[i % segmentValues.length];
                accumulator += segmentDelta;
                count++;
            }
            return accumulator / count;
        }
    }
}