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
package com.aitusoftware.aether.event;

import java.util.List;

import com.aitusoftware.aether.annotation.CallerOwned;
import com.aitusoftware.aether.model.SystemCounters;
import com.aitusoftware.aether.model.PublisherCounterSet;
import com.aitusoftware.aether.model.SubscriberCounterSet;

/**
 * Defines a listener to snapshots of a MediaDriver's counters.
 */
public interface CounterSnapshotListener
{
    /**
     * Called when a complete snapshot of the counters has been built.
     *
     * @param label label of the MediaDriver
     * @param timestamp timestamp of the snapshot
     * @param publisherCounters counters describing publisher state
     * @param subscriberCounters counters describing subscriber state
     * @param systemCounters counters describing system state
     */
    void onSnapshot(
        String label,
        long timestamp,
        @CallerOwned List<PublisherCounterSet> publisherCounters,
        @CallerOwned List<SubscriberCounterSet> subscriberCounters,
        @CallerOwned SystemCounters systemCounters);
}