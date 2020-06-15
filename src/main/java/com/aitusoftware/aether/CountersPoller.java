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
package com.aitusoftware.aether;

import static io.aeron.CncFileDescriptor.CNC_FILE;
import static io.aeron.CncFileDescriptor.CNC_VERSION;
import static io.aeron.CncFileDescriptor.cncVersionOffset;
import static io.aeron.CncFileDescriptor.createCountersMetaDataBuffer;
import static io.aeron.CncFileDescriptor.createCountersValuesBuffer;
import static io.aeron.CncFileDescriptor.createMetaDataBuffer;

import java.io.File;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.MappedByteBuffer;
import java.nio.channels.FileChannel;
import java.nio.file.StandardOpenOption;

import com.aitusoftware.aether.event.CounterValueListener;

import org.agrona.DirectBuffer;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.status.CountersReader;

import io.aeron.driver.status.PublisherLimit;
import io.aeron.driver.status.PublisherPos;
import io.aeron.driver.status.ReceiverHwm;
import io.aeron.driver.status.ReceiverPos;
import io.aeron.driver.status.SenderBpe;
import io.aeron.driver.status.SenderLimit;
import io.aeron.driver.status.SenderPos;
import io.aeron.driver.status.SubscriberPos;
import io.aeron.driver.status.SystemCounterDescriptor;

final class CountersPoller implements Agent, CountersReader.MetaData
{
    private static final long POLL_INTERVAL_MS = 1_000;
    private final CounterValueListener counterValueListener;
    private final CountersReader countersReader;
    private final StringBuilder labelBuffer = new StringBuilder();
    private final String label;
    private final EpochClock epochClock;
    private long lastPollMs = 0L;
    private int countersRead;

    CountersPoller(
        final CounterValueListener counterValueListener,
        final String label, final String aeronDirectoryName,
        final EpochClock epochClock)
    {
        this.counterValueListener = counterValueListener;
        this.label = label;
        this.epochClock = epochClock;
        final File cncFile = new File(aeronDirectoryName, CNC_FILE);

        final MappedByteBuffer cncByteBuffer = mapExistingFileReadOnly(cncFile);
        final DirectBuffer cncMetaData = createMetaDataBuffer(cncByteBuffer);
        final int cncVersion = cncMetaData.getInt(cncVersionOffset(0));

        if (CNC_VERSION != cncVersion)
        {
            throw new IllegalStateException("CnC version not supported: file version=" + cncVersion);
        }

        countersReader = new CountersReader(
            createCountersMetaDataBuffer(cncByteBuffer, cncMetaData),
            createCountersValuesBuffer(cncByteBuffer, cncMetaData));
    }

    @Override
    public int doWork()
    {
        countersRead = 0;
        if (epochClock.time() > lastPollMs + POLL_INTERVAL_MS)
        {
            lastPollMs = epochClock.time();
            countersReader.forEach(this);
            counterValueListener.onEndOfBatch(label);
        }
        return countersRead;
    }

    @Override
    public void accept(final int counterId, final int typeId, final DirectBuffer keyBuffer, final String label)
    {
        countersRead++;
        labelBuffer.setLength(0);
        int streamId = -1;
        int sessionId = -1;
        long registrationId = -1;
        boolean isMonitoredType = isMonitoredSystemCounter(counterId, typeId);
        final String[] tokens = label.split(" ");
        switch (typeId)
        {
            // publisher counters
            case SenderLimit.SENDER_LIMIT_TYPE_ID:
            case SenderPos.SENDER_POSITION_TYPE_ID:
            case PublisherLimit.PUBLISHER_LIMIT_TYPE_ID:
            case SenderBpe.SENDER_BPE_TYPE_ID:
            case ReceiverHwm.RECEIVER_HWM_TYPE_ID:
            case ReceiverPos.RECEIVER_POS_TYPE_ID:
            case SubscriberPos.SUBSCRIBER_POSITION_TYPE_ID:
                registrationId = Long.parseLong(tokens[1]);
                sessionId = Integer.parseInt(tokens[2]);
                streamId = Integer.parseInt(tokens[3]);
                labelBuffer.append(tokens[4]);
                isMonitoredType = true;
                break;
            case PublisherPos.PUBLISHER_POS_TYPE_ID:
                registrationId = Long.parseLong(tokens[2]);
                sessionId = Integer.parseInt(tokens[3]);
                streamId = Integer.parseInt(tokens[4]);
                labelBuffer.append(tokens[5]);
                isMonitoredType = true;
                break;
        }
        if (isMonitoredType)
        {
            final long value = countersReader.getCounterValue(counterId);
            counterValueListener.onCounterEvent(
                counterId, typeId, labelBuffer, sessionId, streamId, registrationId, value);
        }
    }

    private static boolean isMonitoredSystemCounter(final int counterId, final int typeId)
    {
        return typeId == SystemCounterDescriptor.SYSTEM_COUNTER_TYPE_ID &&
            (
                counterId == SystemCounterDescriptor.BYTES_SENT.id() ||
                counterId == SystemCounterDescriptor.BYTES_RECEIVED.id() ||
                counterId == SystemCounterDescriptor.NAK_MESSAGES_SENT.id() ||
                counterId == SystemCounterDescriptor.NAK_MESSAGES_RECEIVED.id() ||
                counterId == SystemCounterDescriptor.ERRORS.id() ||
                counterId == SystemCounterDescriptor.CLIENT_TIMEOUTS.id());
    }

    @Override
    public String roleName()
    {
        return "counters-poller";
    }

    private MappedByteBuffer mapExistingFileReadOnly(final File cncFile)
    {
        if (!cncFile.exists())
        {
            throw new IllegalStateException("File does not exist: " + cncFile);
        }
        try
        {

            return FileChannel.open(cncFile.toPath(), StandardOpenOption.READ)
                .map(FileChannel.MapMode.READ_ONLY, 0, cncFile.length());
        }
        catch (final IOException e)
        {
            throw new UncheckedIOException(e);
        }
    }
}
