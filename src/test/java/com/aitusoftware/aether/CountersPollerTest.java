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
package com.aitusoftware.aether;

import static com.google.common.truth.Truth.assertThat;

import java.nio.charset.StandardCharsets;
import java.util.ArrayList;
import java.util.List;

import com.aitusoftware.aether.event.CounterValueListener;

import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;
import org.agrona.concurrent.UnsafeBuffer;
import org.junit.jupiter.api.AfterEach;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import io.aeron.Aeron;
import io.aeron.ConcurrentPublication;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.driver.MediaDriver;
import io.aeron.driver.ThreadingMode;
import io.aeron.driver.status.PublisherLimit;
import io.aeron.driver.status.PublisherPos;
import io.aeron.driver.status.ReceiverHwm;
import io.aeron.driver.status.ReceiverPos;
import io.aeron.driver.status.SenderBpe;
import io.aeron.driver.status.SenderLimit;
import io.aeron.driver.status.SenderPos;
import io.aeron.driver.status.SubscriberPos;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;

class CountersPollerTest
{
    private static final String CHANNEL = "aeron:udp?endpoint=localhost:14567";
    private static final int STREAM_ID = 7;

    private final CapturingCounterValueListener valueListener = new CapturingCounterValueListener();
    private CountersPoller countersPoller;
    private MediaDriver mediaDriver;
    private Aeron aeron;
    private ConcurrentPublication publication;
    private Subscription subscription;

    @BeforeEach
    void setUp()
    {
        mediaDriver = MediaDriver.launchEmbedded(new MediaDriver.Context()
            .threadingMode(ThreadingMode.SHARED)
            .sharedIdleStrategy(new SleepingMillisIdleStrategy(1)));
        countersPoller = new CountersPoller(
            valueListener, "label", mediaDriver.aeronDirectoryName(), new SystemEpochClock());
        aeron = Aeron.connect(new Aeron.Context().aeronDirectoryName(mediaDriver.aeronDirectoryName()));
        publication = aeron.addPublication(CHANNEL, STREAM_ID);
        subscription = aeron.addSubscription(CHANNEL, STREAM_ID);
    }

    @AfterEach
    void tearDown()
    {
        CloseHelper.close(aeron);
        CloseHelper.close(mediaDriver);
    }

    @Test
    void shouldReportPublisherAndSubscriberCounters()
    {
        final byte[] payload = "payload".getBytes(StandardCharsets.UTF_8);
        final UnsafeBuffer message = new UnsafeBuffer(payload);
        while (publication.offer(message, 0, payload.length) < 0)
        {
            // spin
        }

        final RecordingFragmentHandler fragmentHandler = new RecordingFragmentHandler();
        while (!fragmentHandler.messageReceived)
        {
            subscription.poll(new FragmentAssembler(fragmentHandler), 100);
        }
        countersPoller.doWork();

        assertThat(valueListener.values.size()).isEqualTo(14);
        assertCaptureContains(PublisherPos.PUBLISHER_POS_TYPE_ID, valueListener.values);
        assertCaptureContains(PublisherLimit.PUBLISHER_LIMIT_TYPE_ID, valueListener.values);
        assertCaptureContains(SenderPos.SENDER_POSITION_TYPE_ID, valueListener.values);
        assertCaptureContains(SenderLimit.SENDER_LIMIT_TYPE_ID, valueListener.values);
        assertCaptureContains(SenderBpe.SENDER_BPE_TYPE_ID, valueListener.values);
        assertCaptureContains(SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID, valueListener.values);
        assertCaptureContains(ReceiverPos.RECEIVER_POS_TYPE_ID, valueListener.values);
        assertCaptureContains(ReceiverHwm.RECEIVER_HWM_TYPE_ID, valueListener.values);
    }

    private static void assertCaptureContains(final int typeId, final List<CapturedCounterValue> values)
    {
        assertThat(values.stream().anyMatch(c ->
        {
            return c.typeId == typeId &&
                c.uri.toString().equals(CHANNEL) &&
                c.streamId == STREAM_ID;
        })).isTrue();
    }

    private static final class CapturingCounterValueListener implements CounterValueListener
    {
        private final List<CapturedCounterValue> values = new ArrayList<>();

        @Override
        public void onCounterEvent(
            final int counterId, final int counterTypeId, final CharSequence channel,
            final int sessionId, final int streamId,
            final long registrationId, final long value)
        {
            values.add(new CapturedCounterValue(counterTypeId, channel.toString(), streamId, value));
        }

        @Override
        public void onEndOfBatch(final String label)
        {
        }
    }

    private static final class CapturedCounterValue
    {
        private final int typeId;
        private final CharSequence uri;
        private final int streamId;
        private final long value;

        CapturedCounterValue(final int typeId, final CharSequence uri, final int streamId, final long value)
        {
            this.typeId = typeId;
            this.uri = uri;
            this.streamId = streamId;
            this.value = value;
        }
    }

    private static final class RecordingFragmentHandler implements FragmentHandler
    {
        boolean messageReceived = false;

        @Override
        public void onFragment(final DirectBuffer buffer, final int offset, final int length, final Header header)
        {
            messageReceived = true;
        }
    }
}