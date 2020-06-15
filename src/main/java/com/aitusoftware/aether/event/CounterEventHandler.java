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

import java.util.ArrayList;
import java.util.List;

import com.aitusoftware.aether.annotation.CallerOwned;
import com.aitusoftware.aether.model.SystemCounters;
import com.aitusoftware.aether.model.PublisherCounterSet;
import com.aitusoftware.aether.model.SubscriberCounterSet;

import org.agrona.concurrent.EpochClock;

import io.aeron.driver.status.PublisherLimit;
import io.aeron.driver.status.PublisherPos;
import io.aeron.driver.status.ReceiverHwm;
import io.aeron.driver.status.ReceiverPos;
import io.aeron.driver.status.SenderBpe;
import io.aeron.driver.status.SenderLimit;
import io.aeron.driver.status.SenderPos;
import io.aeron.driver.status.SubscriberPos;
import io.aeron.driver.status.SystemCounterDescriptor;

public final class CounterEventHandler implements CounterValueListener
{
    private final CounterRepository<PublisherCounterSet> publisherCounterRepository;
    private final CounterRepository<SubscriberCounterSet> subscriberCounterRepository;
    private final CounterSnapshotListener counterSnapshotListener;
    private final EpochClock epochClock;
    private final List<PublisherCounterSet> publisherCounters = new ArrayList<>();
    private final List<SubscriberCounterSet> subscriberCounters = new ArrayList<>();
    private final StringBuilder strippedChannel = new StringBuilder();
    private final SystemCounters systemCounters = new SystemCounters();

    public CounterEventHandler(
        final CounterRepository<PublisherCounterSet> publisherCounterRepository,
        final CounterRepository<SubscriberCounterSet> subscriberCounterRepository,
        final CounterSnapshotListener counterSnapshotListener,
        final EpochClock epochClock)
    {
        this.publisherCounterRepository = publisherCounterRepository;
        this.subscriberCounterRepository = subscriberCounterRepository;
        this.counterSnapshotListener = counterSnapshotListener;
        this.epochClock = epochClock;
    }

    @Override
    public void onCounterEvent(
        final int counterId, final int counterTypeId, @CallerOwned final CharSequence channel,
        final int sessionId, final int streamId, final long registrationId, final long value)
    {
        strippedChannel.setLength(0);
        for (int i = 0; i < channel.length(); i++)
        {
            if (channel.charAt(i) == '|')
            {
                break;
            }
            strippedChannel.append(channel.charAt(i));
        }
        if (strippedChannel.indexOf("aeron:ipc") == 0)
        {
            strippedChannel.setLength("aeron:ipc".length());
        }
        switch (counterTypeId)
        {
            // publisher counters
            case SenderLimit.SENDER_LIMIT_TYPE_ID:
                getPublisherCounters(strippedChannel, sessionId, streamId).senderLimit(value);
                break;
            case SenderPos.SENDER_POSITION_TYPE_ID:
                getPublisherCounters(strippedChannel, sessionId, streamId).senderPosition(value);
                break;
            case PublisherPos.PUBLISHER_POS_TYPE_ID:
                getPublisherCounters(strippedChannel, sessionId, streamId).publisherPosition(value);
                break;
            case PublisherLimit.PUBLISHER_LIMIT_TYPE_ID:
                getPublisherCounters(strippedChannel, sessionId, streamId).publisherLimit(value);
                break;
            case SenderBpe.SENDER_BPE_TYPE_ID:
                getPublisherCounters(strippedChannel, sessionId, streamId).backPressureEvents(value);
                break;
            // subscriber counters
            case ReceiverHwm.RECEIVER_HWM_TYPE_ID:
                getSubscriberCounters(strippedChannel, sessionId, streamId)
                    .receiverHighWaterMark(value);
                break;
            case ReceiverPos.RECEIVER_POS_TYPE_ID:
                getSubscriberCounters(strippedChannel, sessionId, streamId).receiverPosition(value);
                break;
            case SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID:
                getSubscriberCounters(strippedChannel, sessionId, streamId)
                    .subscriberPosition(registrationId, value);
                break;
            //system counters
            case SystemCounterDescriptor.SYSTEM_COUNTER_TYPE_ID:
                final SystemCounterDescriptor descriptor = SystemCounterDescriptor.get(counterId);
                switch (descriptor)
                {
                    case BYTES_SENT:
                        systemCounters.bytesSent(value);
                        break;
                    case BYTES_RECEIVED:
                        systemCounters.bytesReceived(value);
                        break;
                    case NAK_MESSAGES_SENT:
                        systemCounters.naksSent(value);
                        break;
                    case NAK_MESSAGES_RECEIVED:
                        systemCounters.naksReceived(value);
                        break;
                    case ERRORS:
                        systemCounters.errors(value);
                        break;
                    case CLIENT_TIMEOUTS:
                        systemCounters.clientTimeouts(value);
                        break;

                }
                break;
        }
    }

    @Override
    public void onEndOfBatch(final String label)
    {
        publisherCounters.clear();
        subscriberCounters.clear();
        publisherCounterRepository.forEach(publisherCounters::add);
        subscriberCounterRepository.forEach(subscriberCounters::add);

        counterSnapshotListener.onSnapshot(label, epochClock.time(),
            publisherCounters, subscriberCounters, systemCounters);
    }

    private PublisherCounterSet getPublisherCounters(
        final CharSequence channel, final int sessionId, final int streamId)
    {
        return publisherCounterRepository.getOrCreate(channel, sessionId, streamId);
    }

    private SubscriberCounterSet getSubscriberCounters(
        final CharSequence channel, final int sessionId, final int streamId)
    {
        return subscriberCounterRepository.getOrCreate(channel, sessionId, streamId);
    }
}