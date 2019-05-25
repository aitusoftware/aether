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

import java.util.ArrayList;
import java.util.List;
import java.util.Optional;

import com.aitusoftware.aether.event.ConsolePrinter;
import com.aitusoftware.aether.event.CounterEventHandler;
import com.aitusoftware.aether.event.CounterRepository;
import com.aitusoftware.aether.event.CounterSnapshotListener;
import com.aitusoftware.aether.model.PublisherCounterSet;
import com.aitusoftware.aether.model.SubscriberCounterSet;
import com.aitusoftware.aether.transport.CounterSnapshotPublisher;
import com.aitusoftware.aether.transport.CounterSnapshotSubscriber;

import org.agrona.CloseHelper;
import org.agrona.SystemUtil;
import org.agrona.concurrent.Agent;
import org.agrona.concurrent.AgentRunner;
import org.agrona.concurrent.EpochClock;
import org.agrona.concurrent.ShutdownSignalBarrier;
import org.agrona.concurrent.SleepingMillisIdleStrategy;
import org.agrona.concurrent.SystemEpochClock;

import io.aeron.Aeron;
import io.aeron.CommonContext;
import io.aeron.driver.MediaDriver;

/**
 * Agent used to read Aeron counters and publish snapshots to the configured {@code CounterSnapshotListener}.
 */
public final class Aether implements Agent, AutoCloseable
{
    private final CountersPoller[] countersPoller;
    private final AgentRunner agentRunner;
    private final Aeron aeronClient;
    private final MediaDriver mediaDriver;
    private final CounterSnapshotSubscriber counterSnapshotSubscriber;
    private CounterSnapshotPublisher counterSnapshotPublisher;

    /**
     * Construct a new instance with the given context.
     *
     * @param context configuration context
     */
    private Aether(final Context context)
    {
        context.validate();
        if (context.launchEmbeddedMediaDriver())
        {
            mediaDriver = MediaDriver.launchEmbedded();
            aeronClient = Aeron.connect(new Aeron.Context().useConductorAgentInvoker(true)
                .aeronDirectoryName(mediaDriver.aeronDirectoryName()));
        }
        else
        {
            mediaDriver = null;
            aeronClient = Aeron.connect(new Aeron.Context().useConductorAgentInvoker(true)
                .aeronDirectoryName(context.aeronDirectoryName()));
        }
        if (context.threadingMode() == ThreadingMode.THREADED)
        {
            agentRunner = new AgentRunner(new SleepingMillisIdleStrategy(1L), e ->
            {
            },
                aeronClient.addCounter(10000, "aether-errors"), this);
            AgentRunner.startOnThread(agentRunner);
        }
        else
        {
            agentRunner = null;
        }
        final EpochClock epochClock = new SystemEpochClock();
        if (context.transport() == Transport.AERON)
        {
            if (context.mode() == Mode.SUBSCRIBER)
            {
                counterSnapshotSubscriber = new CounterSnapshotSubscriber(
                    new CounterSnapshotSubscriber.Context()
                        .counterSnapshotListener(context.counterSnapshotListener())
                        .aeronClient(aeronClient));
            }
            else
            {
                counterSnapshotPublisher = new CounterSnapshotPublisher(
                    new CounterSnapshotPublisher.Context().aeronClient(aeronClient));
                context.counterSnapshotListener(counterSnapshotPublisher);
                counterSnapshotSubscriber = null;
            }
        }
        else
        {
            counterSnapshotSubscriber = null;
        }
        if (context.transport() == Transport.LOCAL || context.mode() == Mode.PUBLISHER)
        {
            countersPoller = new CountersPoller[context.monitoringLocations().size()];
            final List<MonitoringLocation> monitoringLocations = context.monitoringLocations();
            for (int i = 0; i < monitoringLocations.size(); i++)
            {
                final MonitoringLocation monitoringLocation = monitoringLocations.get(i);
                final CounterEventHandler counterEventHandler = new CounterEventHandler(
                    new CounterRepository<>(PublisherCounterSet::new),
                    new CounterRepository<>(SubscriberCounterSet::new),
                    context.counterSnapshotListener(), epochClock);
                countersPoller[i] = new CountersPoller(
                    counterEventHandler, monitoringLocation.label,
                    monitoringLocation.aeronDirectoryName, epochClock);
            }
        }
        else
        {
            countersPoller = new CountersPoller[0];
        }
    }

    /**
     * Main method to start an Aether agent.
     *
     * @param args configuration files
     */
    public static void main(final String[] args)
    {
        SystemUtil.loadPropertiesFiles(args);
        try (Aether aether = launch(new Context()))
        {
            new ShutdownSignalBarrier().await();
        }
    }

    /**
     * Launches an Aether agent with the provided configuration context.
     *
     * @param context configuration context
     * @return the Aether instance
     */
    public static Aether launch(final Context context)
    {
        return new Aether(context);
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public int doWork()
    {
        int work = 0;
        for (final CountersPoller poller : countersPoller)
        {
            work += poller.doWork();
        }
        if (counterSnapshotSubscriber != null)
        {
            work += counterSnapshotSubscriber.doWork();
        }
        return aeronClient.conductorAgentInvoker().invoke() + work;
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public String roleName()
    {
        return "aether";
    }

    /**
     * {@inheritDoc}
     */
    @Override
    public void close()
    {
        CloseHelper.quietClose(counterSnapshotSubscriber);
        CloseHelper.quietClose(counterSnapshotPublisher);
        CloseHelper.quietClose(agentRunner);
        CloseHelper.quietClose(aeronClient);
        CloseHelper.quietClose(mediaDriver);
    }

    /**
     * Configuration context.
     */
    public static final class Context
    {
        private CounterSnapshotListener counterSnapshotListener = new ConsolePrinter();
        private List<MonitoringLocation> monitoringLocations = null;
        private ThreadingMode threadingMode = ThreadingMode.THREADED;
        private Transport transport = Configuration.transport();
        private boolean launchEmbeddedMediaDriver = true;
        private String aeronDirectoryName = CommonContext.getAeronDirectoryName();
        private Mode mode = Configuration.mode();

        void validate()
        {
            if (transport == Transport.AERON)
            {
                if (mode == Mode.LOCAL)
                {
                    throw new IllegalStateException(
                        "Must specify either PUBLISHER or SUBSCRIBER mode when using AERON transport");
                }
            }
            else if (mode != Mode.LOCAL)
            {
                throw new IllegalStateException("Mode must be LOCAL if transport is LOCAL");
            }
        }

        public Context mode(final Mode mode)
        {
            this.mode = mode;
            return this;
        }

        public Mode mode()
        {
            return mode;
        }

        public Context transport(final Transport transport)
        {
            this.transport = transport;
            return this;
        }

        public Transport transport()
        {
            return transport;
        }

        public Context counterSnapshotListener(final CounterSnapshotListener counterSnapshotListener)
        {
            this.counterSnapshotListener = counterSnapshotListener;
            return this;
        }

        public CounterSnapshotListener counterSnapshotListener()
        {
            return counterSnapshotListener;
        }

        public Context monitoringLocations(final List<MonitoringLocation> monitoringLocations)
        {
            this.monitoringLocations = monitoringLocations;
            return this;
        }

        public List<MonitoringLocation> monitoringLocations()
        {
            if (monitoringLocations == null)
            {
                monitoringLocations = new ArrayList<>();
                final String spec = Configuration.monitoringLocations();
                if (spec.length() != 0)
                {
                    final String[] instances = spec.split(";");
                    for (final String instance : instances)
                    {
                        monitoringLocations.add(new MonitoringLocation(
                            instance.substring(0, instance.indexOf(':')),
                            instance.substring(instance.indexOf(':') + 1)));
                    }
                }
            }

            return monitoringLocations;
        }

        public Context threadingMode(final ThreadingMode threadingMode)
        {
            this.threadingMode = threadingMode;
            return this;
        }

        public ThreadingMode threadingMode()
        {
            return threadingMode;
        }

        public Context launchEmbeddedMediaDriver(final boolean launchEmbeddedMediaDriver)
        {
            this.launchEmbeddedMediaDriver = launchEmbeddedMediaDriver;
            return this;
        }

        public boolean launchEmbeddedMediaDriver()
        {
            return launchEmbeddedMediaDriver;
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

    public static final class MonitoringLocation
    {
        private final String label;
        private final String aeronDirectoryName;

        public MonitoringLocation(final String label, final String aeronDirectoryName)
        {
            this.aeronDirectoryName = aeronDirectoryName;
            this.label = label;
        }
    }

    public static final class Configuration
    {
        public static final String MONITORING_LOCATIONS_PROPERTY_NAME = "aether.monitoringLocations";
        public static final String TRANSPORT_PROPERTY_NAME = "aether.transport";
        public static final String MODE_PROPERTY_NAME = "aether.mode";

        public static String monitoringLocations()
        {
            return System.getProperty(MONITORING_LOCATIONS_PROPERTY_NAME,
                "default:" + CommonContext.getAeronDirectoryName());
        }

        public static Transport transport()
        {
            return Optional.ofNullable(System.getProperty(TRANSPORT_PROPERTY_NAME))
                .map(Transport::valueOf).orElse(Transport.LOCAL);
        }

        public static Mode mode()
        {
            return Optional.ofNullable(System.getProperty(MODE_PROPERTY_NAME))
                .map(Mode::valueOf).orElse(Mode.LOCAL);
        }
    }

    public enum ThreadingMode
    {
        THREADED,
        INVOKER
    }

    public enum Transport
    {
        LOCAL,
        AERON
    }

    public enum Mode
    {
        LOCAL,
        PUBLISHER,
        SUBSCRIBER
    }
}