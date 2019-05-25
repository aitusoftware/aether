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
import com.aitusoftware.aether.event.CounterSnapshotListener;
import com.aitusoftware.aether.model.SystemCounters;
import com.aitusoftware.aether.model.PublisherCounterSet;
import com.aitusoftware.aether.model.SubscriberCounterSet;
import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.Publication;
import org.agrona.CloseHelper;
import org.agrona.ExpandableArrayBuffer;
import org.agrona.MutableDirectBuffer;

import java.util.List;

/**
 * An implementation of {@code CounterSnapshotListener} that publishes to an Aeron {@code Publication}.
 */
public final class CounterSnapshotPublisher implements CounterSnapshotListener, AutoCloseable
{
    private final MutableDirectBuffer buffer = new ExpandableArrayBuffer();
    private final SnapshotSerialiser serialiser = new SnapshotSerialiser();
    private final Publication publication;
    private final Aeron aeronClient;
    private final boolean ownsAeronClient;

    /**
     * Construct a new publisher using runtime configuration.
     */
    public CounterSnapshotPublisher()
    {
        this(new Context());
    }

    /**
     * Construct a new publisher from the supplied context.
     *
     * @param context configuration context
     */
    public CounterSnapshotPublisher(final Context context)
    {
        if (context.aeronClient() == null)
        {
            aeronClient = Aeron.connect(new Aeron.Context()
                .aeronDirectoryName(context.aeronDirectoryName()));
            ownsAeronClient = true;
        }
        else
        {
            aeronClient = context.aeronClient();
            ownsAeronClient = false;
        }

        publication = aeronClient.addPublication(context.aetherChannel(), context.aetherStreamId());
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onSnapshot(
        final String label,
        final long timestamp,
        @CallerOwned final List<PublisherCounterSet> publisherCounters,
        @CallerOwned final List<SubscriberCounterSet> subscriberCounters,
        @CallerOwned final SystemCounters systemCounters)
    {
        final int length = serialiser.serialiseSnapshot(
            label, timestamp, publisherCounters, subscriberCounters, systemCounters, buffer);

        int retryCount = 5;
        long result;
        do
        {
            result = publication.offer(buffer, 0, length);
            if (result == Publication.CLOSED)
            {
                throw new IllegalStateException("Publication closed");
            }
        }
        while (--retryCount != 0 && result < 0);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        CloseHelper.close(publication);
        if (ownsAeronClient)
        {
            CloseHelper.close(aeronClient);
        }
    }

    /**
     * Configuration context.
     */
    public static final class Context
    {
        private Aeron aeronClient;
        private String aetherChannel = ChannelConfig.AETHER_CHANNEL;
        private int aetherStreamId = ChannelConfig.AETHER_STREAM_ID;
        private String aeronDirectoryName = CommonContext.getAeronDirectoryName();

        public Context aeronClient(final Aeron aeronClient)
        {
            this.aeronClient = aeronClient;
            return this;
        }

        public Aeron aeronClient()
        {
            return aeronClient;
        }

        public Context aetherChannel(final String channel)
        {
            this.aetherChannel = channel;
            return this;
        }

        public String aetherChannel()
        {
            return aetherChannel;
        }

        public Context aetherStreamId(final int streamId)
        {
            this.aetherStreamId = streamId;
            return this;
        }

        public int aetherStreamId()
        {
            return aetherStreamId;
        }

        public Context aeronDirectoryName(final String aeronDirectoryName)
        {
            this.aeronDirectoryName = aeronDirectoryName;
            return this;
        }

        public String aeronDirectoryName()
        {
            return aeronDirectoryName;
        }
    }
}