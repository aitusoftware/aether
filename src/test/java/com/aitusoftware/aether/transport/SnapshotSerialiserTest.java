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
package com.aitusoftware.aether.transport;

import com.aitusoftware.aether.event.CounterSnapshotListener;
import com.aitusoftware.aether.model.SystemCounters;
import com.aitusoftware.aether.model.PublisherCounterSet;
import com.aitusoftware.aether.model.SubscriberCounterSet;
import org.agrona.ExpandableArrayBuffer;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.Test;

import java.util.Arrays;
import java.util.Collections;
import java.util.List;

import static com.google.common.truth.Truth.assertThat;

class SnapshotSerialiserTest
{
    private static final String LABEL = "label-0";
    private static final long TIMESTAMP = 1234567890333L;
    private final SnapshotSerialiser serialiser = new SnapshotSerialiser();
    private final SnapshotDeserialiser deserialiser = new SnapshotDeserialiser();
    private final SystemCounters systemCounters = new SystemCounters();

    @BeforeEach
    void setUp()
    {
        systemCounters.bytesSent(1);
        systemCounters.bytesReceived(2);
        systemCounters.naksSent(3);
        systemCounters.naksReceived(4);
        systemCounters.errors(5);
        systemCounters.clientTimeouts(6);
    }

    @Test
    void shouldConvert()
    {
        final ExpandableArrayBuffer buffer = new ExpandableArrayBuffer();
        final int length = serialiser.serialiseSnapshot(LABEL, TIMESTAMP, publishers(), subscribers(),
            systemCounters, buffer);
        deserialiser.deserialiseSnapshot(buffer, 0, new SnapshotAssertion());
    }

    private List<SubscriberCounterSet> subscribers()
    {
        final SubscriberCounterSet s0 = new SubscriberCounterSet();
        s0.reset("chan-1", 2, 7);
        s0.receiverHighWaterMark(1234L);
        return Collections.singletonList(s0);
    }

    private List<PublisherCounterSet> publishers()
    {
        final PublisherCounterSet p0 = new PublisherCounterSet();
        p0.reset("chan-1", 2, 7);
        p0.senderPosition(1234L);
        final PublisherCounterSet p1 = new PublisherCounterSet();
        p1.reset("chan-2", 5, 11);
        p1.publisherLimit(1234L);
        return Arrays.asList(p0, p1);
    }

    private class SnapshotAssertion implements CounterSnapshotListener
    {
        @Override
        public void onSnapshot(
            final String label,
            final long timestamp,
            final List<PublisherCounterSet> publisherCounters,
            final List<SubscriberCounterSet> subscriberCounters,
            final SystemCounters systemCounters)
        {
            assertThat(label).isEqualTo(LABEL);
            assertThat(timestamp).isEqualTo(TIMESTAMP);
            assertThat(publisherCounters.size()).isEqualTo(publishers().size());
            assertThat(subscriberCounters.size()).isEqualTo(subscribers().size());
            assertThat(subscriberCounters.get(0).channel().toString()).isEqualTo("chan-1");
            assertThat(subscriberCounters.get(0).receiverHighWaterMark()).isEqualTo(1234L);
            assertThat(publisherCounters.get(0).channel().toString()).isEqualTo("chan-1");
            assertThat(publisherCounters.get(0).senderPosition()).isEqualTo(1234L);
            assertThat(publisherCounters.get(1).channel().toString()).isEqualTo("chan-2");
            assertThat(publisherCounters.get(1).publisherLimit()).isEqualTo(1234L);

            assertThat(systemCounters.bytesSent()).isEqualTo((1L));
            assertThat(systemCounters.bytesReceived()).isEqualTo((2L));
            assertThat(systemCounters.naksSent()).isEqualTo((3L));
            assertThat(systemCounters.naksReceived()).isEqualTo((4L));
            assertThat(systemCounters.errors()).isEqualTo((5L));
            assertThat(systemCounters.clientTimeouts()).isEqualTo((6L));
        }
    }
}