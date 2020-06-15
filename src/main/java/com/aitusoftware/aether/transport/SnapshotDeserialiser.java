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

import java.util.ArrayList;
import java.util.List;

import com.aitusoftware.aether.event.CounterSnapshotListener;
import com.aitusoftware.aether.model.PublisherCounterSet;
import com.aitusoftware.aether.model.SubscriberCounterSet;
import com.aitusoftware.aether.model.SystemCounters;

import org.agrona.DirectBuffer;

final class SnapshotDeserialiser
{
    private final StringBuilder charBuffer = new StringBuilder();
    private final SystemCounters systemCounters = new SystemCounters();

    void deserialiseSnapshot(
        final DirectBuffer buffer,
        final int offset,
        final CounterSnapshotListener listener)
    {
        int relativeOffset = offset;
        final int headerId = buffer.getInt(relativeOffset);
        relativeOffset += Integer.BYTES;
        final byte version = buffer.getByte(relativeOffset);
        relativeOffset += Byte.BYTES;
        if (Versions.SNAPSHOT_HEADER_ID != headerId)
        {
            throw new IllegalArgumentException("Unknown message type: " + headerId);
        }
        if (Versions.VERSION != version)
        {
            throw new IllegalArgumentException("Unknown version: " + version);
        }
        final int labelLength = buffer.getInt(relativeOffset);
        relativeOffset += Integer.BYTES;
        final StringBuilder label = new StringBuilder();
        for (int i = 0; i < labelLength; i++)
        {
            label.append(buffer.getChar(relativeOffset));
            relativeOffset += Character.BYTES;
        }
        final long timestamp = buffer.getLong(relativeOffset);
        relativeOffset += Long.BYTES;

        systemCounters.bytesSent(buffer.getLong(relativeOffset));
        relativeOffset += Long.BYTES;
        systemCounters.bytesReceived(buffer.getLong(relativeOffset));
        relativeOffset += Long.BYTES;
        systemCounters.naksSent(buffer.getLong(relativeOffset));
        relativeOffset += Long.BYTES;
        systemCounters.naksReceived(buffer.getLong(relativeOffset));
        relativeOffset += Long.BYTES;
        systemCounters.errors(buffer.getLong(relativeOffset));
        relativeOffset += Long.BYTES;
        systemCounters.clientTimeouts(buffer.getLong(relativeOffset));
        relativeOffset += Long.BYTES;

        final List<PublisherCounterSet> publisherCounters = new ArrayList<>();
        relativeOffset = readPublisherCounters(relativeOffset, publisherCounters, buffer);
        final List<SubscriberCounterSet> subscriberCounters = new ArrayList<>();
        relativeOffset = readSubscriberCounters(relativeOffset, subscriberCounters, buffer);

        listener.onSnapshot(label.toString(), timestamp, publisherCounters, subscriberCounters, systemCounters);
    }

    private int readSubscriberCounters(
        final int offset,
        final List<SubscriberCounterSet> subscriberCounters,
        final DirectBuffer buffer)
    {
        int localOffset = offset;
        final int count = buffer.getInt(localOffset);
        localOffset += Integer.BYTES;

        for (int i = 0; i < count; i++)
        {
            final int labelLength = buffer.getInt(localOffset);

            localOffset += Integer.BYTES;
            charBuffer.setLength(0);
            for (int c = 0; c < labelLength; c++)
            {
                charBuffer.append(buffer.getChar(localOffset));
                localOffset += Character.BYTES;
            }
            final int streamId = buffer.getInt(localOffset);
            localOffset += Integer.BYTES;
            final int sessionId = buffer.getInt(localOffset);
            localOffset += Integer.BYTES;

            final int subscriberCount = buffer.getInt(localOffset);
            localOffset += Integer.BYTES;
            final SubscriberCounterSet counterSet = new SubscriberCounterSet();
            counterSet.reset(charBuffer.toString(), sessionId, streamId);

            for (int j = 0; j < subscriberCount; j++)
            {
                final long regId = buffer.getLong(localOffset);
                localOffset += Long.BYTES;
                final long value = buffer.getLong(localOffset);
                localOffset += Long.BYTES;
                counterSet.subscriberPosition(regId, value);
            }

            final long receiverPosition = buffer.getLong(localOffset);
            localOffset += Long.BYTES;
            final long receiverHighWaterMark = buffer.getLong(localOffset);
            localOffset += Long.BYTES;

            counterSet.receiverPosition(receiverPosition);
            counterSet.receiverHighWaterMark(receiverHighWaterMark);
            subscriberCounters.add(counterSet);
        }
        return localOffset;
    }

    private int readPublisherCounters(
        final int offset,
        final List<PublisherCounterSet> publisherCounters,
        final DirectBuffer buffer)
    {
        int localOffset = offset;
        final int count = buffer.getInt(localOffset);
        localOffset += Integer.BYTES;
        for (int i = 0; i < count; i++)
        {
            final int labelLength = buffer.getInt(localOffset);
            localOffset += Integer.BYTES;
            charBuffer.setLength(0);
            for (int c = 0; c < labelLength; c++)
            {
                charBuffer.append(buffer.getChar(localOffset));
                localOffset += Character.BYTES;
            }
            final int streamId = buffer.getInt(localOffset);
            localOffset += Integer.BYTES;
            final int sessionId = buffer.getInt(localOffset);
            localOffset += Integer.BYTES;

            final long publisherPosition = buffer.getLong(localOffset);
            localOffset += Long.BYTES;
            final long backPressureEvents = buffer.getLong(localOffset);
            localOffset += Long.BYTES;
            final long senderPosition = buffer.getLong(localOffset);
            localOffset += Long.BYTES;
            final long senderLimit = buffer.getLong(localOffset);
            localOffset += Long.BYTES;
            final long publisherLimit = buffer.getLong(localOffset);
            localOffset += Long.BYTES;

            final PublisherCounterSet counterSet = new PublisherCounterSet();
            counterSet.reset(charBuffer.toString(), sessionId, streamId);
            counterSet.publisherPosition(publisherPosition);
            counterSet.publisherLimit(publisherLimit);
            counterSet.backPressureEvents(backPressureEvents);
            counterSet.senderPosition(senderPosition);
            counterSet.senderLimit(senderLimit);

            publisherCounters.add(counterSet);
        }
        return localOffset;
    }
}
