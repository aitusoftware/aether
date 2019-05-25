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
package com.aitusoftware.aether.transport;

import com.aitusoftware.aether.annotation.CallerOwned;
import com.aitusoftware.aether.model.PublisherCounterSet;
import com.aitusoftware.aether.model.SubscriberCounterSet;
import com.aitusoftware.aether.model.SystemCounters;

import org.agrona.MutableDirectBuffer;

import java.util.List;
import java.util.Map;

final class SnapshotSerialiser
{
    int serialiseSnapshot(
        final String label,
        final long timestamp,
        @CallerOwned final List<PublisherCounterSet> publisherCounters,
        @CallerOwned final List<SubscriberCounterSet> subscriberCounters,
        @CallerOwned final SystemCounters systemCounters,
        final MutableDirectBuffer buffer)
    {
        int offset = 0;
        buffer.putInt(offset, Versions.SNAPSHOT_HEADER_ID);
        offset += Integer.BYTES;
        buffer.putByte(offset, Versions.VERSION);
        offset += Byte.BYTES;
        buffer.putInt(offset, label.length());
        offset += Integer.BYTES;
        for (int i = 0; i < label.length(); i++)
        {
            buffer.putChar(offset, label.charAt(i));
            offset += Character.BYTES;
        }
        buffer.putLong(offset, timestamp);
        offset += Long.BYTES;
        buffer.putLong(offset, systemCounters.bytesSent());
        offset += Long.BYTES;
        buffer.putLong(offset, systemCounters.bytesReceived());
        offset += Long.BYTES;
        buffer.putLong(offset, systemCounters.naksSent());
        offset += Long.BYTES;
        buffer.putLong(offset, systemCounters.naksReceived());
        offset += Long.BYTES;
        buffer.putLong(offset, systemCounters.errors());
        offset += Long.BYTES;
        buffer.putLong(offset, systemCounters.clientTimeouts());
        offset += Long.BYTES;
        offset = writePublisherCounters(offset, publisherCounters, buffer);
        offset = writeSubscriberCounters(offset, subscriberCounters, buffer);
        return offset;
    }

    private static int writeSubscriberCounters(
        final int offset,
        final List<SubscriberCounterSet> subscriberCounters,
        final MutableDirectBuffer buffer)
    {
        int localOffset = offset;
        buffer.putInt(localOffset, subscriberCounters.size());
        localOffset += Integer.BYTES;
        for (final SubscriberCounterSet subscriberCounter : subscriberCounters)
        {
            buffer.putInt(localOffset, subscriberCounter.channel().length());
            localOffset += Integer.BYTES;
            for (int i = 0; i < subscriberCounter.channel().length(); i++)
            {
                buffer.putChar(localOffset, subscriberCounter.channel().charAt(i));
                localOffset += Character.BYTES;
            }
            buffer.putInt(localOffset, subscriberCounter.streamId());
            localOffset += Integer.BYTES;
            buffer.putInt(localOffset, subscriberCounter.sessionId());
            localOffset += Integer.BYTES;

            buffer.putInt(localOffset, subscriberCounter.subscriberCount());
            localOffset += Integer.BYTES;
            for (final Map.Entry<Long, Long> positions : subscriberCounter.subscriberPositions().entrySet())
            {
                buffer.putLong(localOffset, positions.getKey());
                localOffset += Long.BYTES;
                buffer.putLong(localOffset, positions.getValue());
                localOffset += Long.BYTES;
            }

            buffer.putLong(localOffset, subscriberCounter.receiverPosition());
            localOffset += Long.BYTES;
            buffer.putLong(localOffset, subscriberCounter.receiverHighWaterMark());
            localOffset += Long.BYTES;
        }
        return localOffset;
    }

    private static int writePublisherCounters(
        final int offset,
        final List<PublisherCounterSet> publisherCounters,
        final MutableDirectBuffer buffer)
    {
        int localOffset = offset;
        buffer.putInt(localOffset, publisherCounters.size());
        localOffset += Integer.BYTES;
        for (final PublisherCounterSet publisherCounter : publisherCounters)
        {
            buffer.putInt(localOffset, publisherCounter.channel().length());
            localOffset += Integer.BYTES;
            for (int i = 0; i < publisherCounter.channel().length(); i++)
            {
                buffer.putChar(localOffset, publisherCounter.channel().charAt(i));
                localOffset += Character.BYTES;
            }
            buffer.putInt(localOffset, publisherCounter.streamId());
            localOffset += Integer.BYTES;
            buffer.putInt(localOffset, publisherCounter.sessionId());
            localOffset += Integer.BYTES;

            buffer.putLong(localOffset, publisherCounter.publisherPosition());
            localOffset += Long.BYTES;
            buffer.putLong(localOffset, publisherCounter.backPressureEvents());
            localOffset += Long.BYTES;
            buffer.putLong(localOffset, publisherCounter.senderPosition());
            localOffset += Long.BYTES;
            buffer.putLong(localOffset, publisherCounter.senderLimit());
            localOffset += Long.BYTES;
            buffer.putLong(localOffset, publisherCounter.publisherLimit());
            localOffset += Long.BYTES;
        }
        return localOffset;
    }
}
