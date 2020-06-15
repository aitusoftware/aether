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

import static com.google.common.truth.Truth.assertThat;

import java.util.Arrays;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import java.util.Set;

import com.aitusoftware.aether.model.PublisherCounterSet;
import com.aitusoftware.aether.model.ChannelSessionKey;
import com.aitusoftware.aether.model.SubscriberCounterSet;
import com.google.common.collect.Sets;

import org.agrona.concurrent.SystemEpochClock;
import org.junit.jupiter.api.Test;

import io.aeron.driver.status.PublisherPos;
import io.aeron.driver.status.ReceiverHwm;
import io.aeron.driver.status.ReceiverPos;
import io.aeron.driver.status.SubscriberPos;

class CounterEventHandlerTest
{
    private static final String CHANNEL = "CHANNEL";
    private static final int SESSION_ID = -1200003;
    private static final int STREAM_ID = 37;
    private static final String CHANNEL_2 = "CHANNEL_2";
    private static final String LABEL = "label";
    private static final int VALUE = 101_888;
    private static final List<CounterValue> TEST_DATA = Arrays.asList(
        new CounterValue(PublisherPos.PUBLISHER_POS_TYPE_ID, CHANNEL, 11,
        SESSION_ID, STREAM_ID, VALUE),
        new CounterValue(PublisherPos.PUBLISHER_POS_TYPE_ID, CHANNEL_2, 12,
        SESSION_ID, STREAM_ID, VALUE),
        new CounterValue(SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID, CHANNEL, 1,
        SESSION_ID, STREAM_ID, VALUE),
        new CounterValue(SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID, CHANNEL, 2,
        SESSION_ID, STREAM_ID, VALUE),
        new CounterValue(SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID, CHANNEL, 3,
        SESSION_ID, STREAM_ID, VALUE),
        new CounterValue(ReceiverHwm.RECEIVER_HWM_TYPE_ID, CHANNEL, 8,
        SESSION_ID, STREAM_ID, VALUE),
        new CounterValue(ReceiverPos.RECEIVER_POS_TYPE_ID, CHANNEL, 8,
        SESSION_ID, STREAM_ID, VALUE),
        new CounterValue(SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID, CHANNEL_2, 4,
        SESSION_ID, STREAM_ID, VALUE),
        new CounterValue(SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID, CHANNEL_2, 5,
        SESSION_ID, STREAM_ID, VALUE),
        new CounterValue(ReceiverHwm.RECEIVER_HWM_TYPE_ID, CHANNEL_2, 9,
        SESSION_ID, STREAM_ID, VALUE),
        new CounterValue(ReceiverPos.RECEIVER_POS_TYPE_ID, CHANNEL_2, 10,
        SESSION_ID, STREAM_ID, VALUE));

    private final SystemSnapshot systemSnapshot = new SystemSnapshot();
    private final CounterEventHandler counterEventHandler = new CounterEventHandler(
        new CounterRepository<>(PublisherCounterSet::new),
        new CounterRepository<>(SubscriberCounterSet::new),
        systemSnapshot, new SystemEpochClock());

    @Test
    void shouldBuildModel()
    {
        for (final CounterValue testDatum : TEST_DATA)
        {
            counterEventHandler.onCounterEvent(
                0, testDatum.typeId, testDatum.channel,
                testDatum.sessionId, testDatum.streamId,
                testDatum.registrationId, testDatum.value);
        }
        counterEventHandler.onEndOfBatch(LABEL);

        final Map<StreamKey, Map<ChannelSessionKey, Set<ChannelSessionKey>>> connectionsByStream =
            systemSnapshot.getConnectionsByStream();
        final Map<StreamKey, Map<ChannelSessionKey, Set<ChannelSessionKey>>> expected =
            new HashMap<>();
        final Map<ChannelSessionKey, Set<ChannelSessionKey>> channelOneMap = new HashMap<>();
        expected.put(new StreamKey(CHANNEL, STREAM_ID), channelOneMap);
        channelOneMap.put(new ChannelSessionKey(LABEL, CHANNEL, STREAM_ID, SESSION_ID),
            Sets.newHashSet(new ChannelSessionKey(LABEL, CHANNEL, STREAM_ID, SESSION_ID)));
        final Map<ChannelSessionKey, Set<ChannelSessionKey>> channelTwoMap = new HashMap<>();
        expected.put(new StreamKey(CHANNEL_2, STREAM_ID), channelTwoMap);
        channelTwoMap.put(new ChannelSessionKey(LABEL, CHANNEL_2, STREAM_ID, SESSION_ID),
            Sets.newHashSet(new ChannelSessionKey(LABEL, CHANNEL_2, STREAM_ID, SESSION_ID)));
        assertThat(connectionsByStream).isEqualTo(expected);

        assertThat(systemSnapshot.getPublisherCounterSet(
            new ChannelSessionKey(LABEL, CHANNEL, STREAM_ID, SESSION_ID)).publisherPosition())
            .isEqualTo(VALUE);
        assertThat(systemSnapshot.getPublisherCounterSet(
            new ChannelSessionKey(LABEL, CHANNEL_2, STREAM_ID, SESSION_ID)).publisherPosition())
            .isEqualTo(VALUE);

        final SubscriberCounterSet subscriberCounterSet =
            systemSnapshot.getSubscriberCounterSet(new ChannelSessionKey(LABEL, CHANNEL, STREAM_ID, SESSION_ID));
        assertThat(subscriberCounterSet.receiverPosition()).isEqualTo(VALUE);
        assertThat(subscriberCounterSet.receiverHighWaterMark()).isEqualTo(VALUE);
        assertThat(subscriberCounterSet.subscriberPositions().size()).isEqualTo(3);
        assertThat(subscriberCounterSet.subscriberPositions().keySet()).contains(1L);
        assertThat(subscriberCounterSet.subscriberPositions().keySet()).contains(2L);
        assertThat(subscriberCounterSet.subscriberPositions().keySet()).contains(3L);
    }

    private static final class CounterValue
    {
        private final int typeId;
        private final String channel;
        private final int registrationId;
        private final int sessionId;
        private final int streamId;
        private final long value;

        CounterValue(
            final int typeId,
            final String channel,
            final int registrationId,
            final int sessionId,
            final int streamId,
            final long value)
        {
            this.typeId = typeId;
            this.channel = channel;
            this.registrationId = registrationId;
            this.sessionId = sessionId;
            this.streamId = streamId;
            this.value = value;
        }
    }
}