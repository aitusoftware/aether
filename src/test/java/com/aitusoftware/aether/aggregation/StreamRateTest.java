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
package com.aitusoftware.aether.aggregation;

import org.junit.jupiter.api.Test;

import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.concurrent.TimeUnit;

import static com.google.common.truth.Truth.assertThat;

class StreamRateTest
{
    private static final long BASE_TIME = System.currentTimeMillis();
    private final StreamRate streamRate = new StreamRate(
        Arrays.asList(
        new RateBucket(5, TimeUnit.SECONDS),
        new RateBucket(1, TimeUnit.MINUTES)));

    @Test
    void shouldCalculateSameAverageRateInMultipleBuckets()
    {
        for (int i = 0; i < 700; i++)
        {
            streamRate.streamPosition(BASE_TIME + TimeUnit.SECONDS.toMillis(i), i * 5000);
        }

        final List<Rate> rates = new ArrayList<>();
        streamRate.consumeRates(((duration, durationUnit, bytesPerUnit) ->
        {
            rates.add(new Rate(duration, durationUnit, bytesPerUnit));
        }));

        assertThat(rates.size()).isEqualTo(2);
        assertThat(rates.get(0).bytesPerSecond).isEqualTo(5000L);
        assertThat(rates.get(1).bytesPerSecond).isEqualTo(5000L);
    }

    @Test
    void shouldMaintainAverageRate()
    {
        streamRate.streamPosition(BASE_TIME, 0);
        streamRate.streamPosition(BASE_TIME + TimeUnit.SECONDS.toMillis(1), 2000);
        streamRate.streamPosition(BASE_TIME + TimeUnit.SECONDS.toMillis(2), 4999);
        streamRate.streamPosition(BASE_TIME + TimeUnit.SECONDS.toMillis(3), 5000);
        streamRate.streamPosition(BASE_TIME + TimeUnit.SECONDS.toMillis(4), 6000);
        streamRate.streamPosition(BASE_TIME + TimeUnit.SECONDS.toMillis(5), 7000);
        final List<Rate> rates = new ArrayList<>();
        streamRate.consumeRates(((duration, durationUnit, bytesPerUnit) ->
        {
            rates.add(new Rate(duration, durationUnit, bytesPerUnit));
        }));

        assertThat(rates.size()).isEqualTo(2);
        assertThat(rates.get(0).duration).isEqualTo(5L);
        assertThat(rates.get(0).durationUnit).isEqualTo(TimeUnit.SECONDS);
        assertThat(rates.get(0).bytesPerSecond).isEqualTo(1250L);
    }

    @Test
    void shouldCalculateAverageRatesOfMultipleTimeWindows()
    {
        streamRate.streamPosition(BASE_TIME, 0);
        for (int i = 1; i <= 600 / 5; i++)
        {
            streamRate.streamPosition(BASE_TIME + TimeUnit.SECONDS.toMillis(i),
                (i * i) + 2000 + i * 20);
        }
        final List<Rate> rates = new ArrayList<>();
        streamRate.consumeRates(((duration, durationUnit, bytesPerUnit) ->
        {
            rates.add(new Rate(duration, durationUnit, bytesPerUnit));
        }));

        assertThat(rates.size()).isEqualTo(2);
        assertThat(rates.get(1).bytesPerSecond).isLessThan(rates.get(0).bytesPerSecond);
        assertThat(rates.get(0).duration).isEqualTo(5L);
        assertThat(rates.get(0).durationUnit).isEqualTo(TimeUnit.SECONDS);
        assertThat(rates.get(0).bytesPerSecond).isEqualTo(256L);
        assertThat(rates.get(1).duration).isEqualTo(1L);
        assertThat(rates.get(1).durationUnit).isEqualTo(TimeUnit.MINUTES);
        assertThat(rates.get(1).bytesPerSecond).isEqualTo(201L);
    }

    @Test
    void shouldFilterUnnecessaryUpdates()
    {
        streamRate.streamPosition(BASE_TIME, 0);
        streamRate.streamPosition(BASE_TIME + TimeUnit.SECONDS.toMillis(1), 2000);
        streamRate.streamPosition(BASE_TIME + TimeUnit.SECONDS.toMillis(1) + 5, 2000);
        streamRate.streamPosition(BASE_TIME + TimeUnit.SECONDS.toMillis(1) + 15, 2200);
        streamRate.streamPosition(BASE_TIME + TimeUnit.SECONDS.toMillis(1) + 30, 2650);
        streamRate.streamPosition(BASE_TIME + TimeUnit.SECONDS.toMillis(2), 4000);
        final List<Rate> rates = new ArrayList<>();
        streamRate.consumeRates(((duration, durationUnit, bytesPerUnit) ->
        {
            rates.add(new Rate(duration, durationUnit, bytesPerUnit));
        }));

        assertThat(rates.size()).isEqualTo(2);
        assertThat(rates.get(0).duration).isEqualTo(5L);
        assertThat(rates.get(0).durationUnit).isEqualTo(TimeUnit.SECONDS);
        assertThat(rates.get(0).bytesPerSecond).isEqualTo(2000L);
    }

    private static final class Rate
    {
        private final long duration;
        private final TimeUnit durationUnit;
        private final long bytesPerSecond;

        Rate(final long duration, final TimeUnit durationUnit, final long bytesPerSecond)
        {
            this.duration = duration;
            this.durationUnit = durationUnit;
            this.bytesPerSecond = bytesPerSecond;
        }
    }
}