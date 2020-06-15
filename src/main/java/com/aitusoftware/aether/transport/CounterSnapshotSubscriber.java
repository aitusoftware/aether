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
import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.FragmentAssembler;
import io.aeron.Subscription;
import io.aeron.logbuffer.FragmentHandler;
import io.aeron.logbuffer.Header;
import org.agrona.CloseHelper;
import org.agrona.DirectBuffer;

/**
 * A listener to data published by a {@code CounterSnapshotPublisher}.
 */
public final class CounterSnapshotSubscriber implements FragmentHandler, AutoCloseable
{
    private final SnapshotDeserialiser deserialiser = new SnapshotDeserialiser();
    private final FragmentAssembler fragmentAssembler = new FragmentAssembler(this);
    private final Subscription subscription;
    private final CounterSnapshotListener counterSnapshotListener;
    private final Aeron aeronClient;
    private final boolean ownsAeronClient;

    /**
     * Creates a new subscriber using runtime configuration.
     */
    public CounterSnapshotSubscriber()
    {
        this(new Context());
    }

    /**
     * Creates a new subscriber from the supplied context.
     * @param context configuration context
     */
    public CounterSnapshotSubscriber(final Context context)
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

        subscription = aeronClient.addSubscription(context.aetherChannel(), context.aetherStreamId());
        counterSnapshotListener = context.counterSnapshotListener();
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close() throws Exception
    {
        CloseHelper.close(subscription);
        if (ownsAeronClient)
        {
            CloseHelper.close(aeronClient);
        }
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void onFragment(
        final DirectBuffer buffer,
        final int offset,
        final int length,
        final Header header)
    {
        deserialiser.deserialiseSnapshot(buffer, offset, counterSnapshotListener);
    }

    /**
     * Poll the Aeron {@code Subscription}.
     *
     * @return number of fragments processed
     */
    public int doWork()
    {
        return subscription.poll(fragmentAssembler, 100);
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
        private CounterSnapshotListener counterSnapshotListener;

        public Context counterSnapshotListener(final CounterSnapshotListener counterSnapshotListener)
        {
            this.counterSnapshotListener = counterSnapshotListener;
            return this;
        }

        public CounterSnapshotListener counterSnapshotListener()
        {
            return counterSnapshotListener;
        }

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
