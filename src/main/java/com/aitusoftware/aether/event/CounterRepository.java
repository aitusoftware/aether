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

import java.util.HashMap;
import java.util.Map;
import java.util.function.Consumer;
import java.util.function.Supplier;

import com.aitusoftware.aether.model.SessionKeyed;

public final class CounterRepository<T extends SessionKeyed>
{
    private final Map<Key, T> countersMap = new HashMap<>();
    private final Supplier<T> factory;

    public CounterRepository(final Supplier<T> factory)
    {
        this.factory = factory;
    }

    T getOrCreate(
        final CharSequence channel, final int sessionId, final int streamId)
    {
        final Key key = new Key().set(channel, sessionId, streamId);
        T counters = countersMap.get(key);
        if (counters == null)
        {
            counters = factory.get();
            counters.reset(channel, sessionId, streamId);
            countersMap.put(key, counters);
        }
        return counters;
    }

    void forEach(final Consumer<T> consumer)
    {
        countersMap.values().forEach(consumer);
    }

    private static final class Key
    {
        private StringBuilder channel = new StringBuilder();
        private int sessionId;
        private int streamId;

        Key set(final CharSequence channel, final int sessionId, final int streamId)
        {
            this.channel.setLength(0);
            this.channel.append(channel);
            this.sessionId = sessionId;
            this.streamId = streamId;

            return this;
        }

        @Override
        public boolean equals(final Object o)
        {
            if (this == o)
            {
                return true;
            }
            if (o == null || getClass() != o.getClass())
            {
                return false;
            }
            final Key key = (Key)o;
            return sessionId == key.sessionId &&
                streamId == key.streamId &&
                CharSequenceUtil.charSequencesEqual(channel, key.channel);
        }

        @Override
        public int hashCode()
        {
            int hashCode = hashCharSequence(channel);
            hashCode = 31 * hashCode + sessionId;
            hashCode = 31 * hashCode + streamId;
            return hashCode;
        }

        private int hashCharSequence(final CharSequence charSequence)
        {
            int hashCode = 0;
            for (int i = 0; i < charSequence.length(); i++)
            {
                hashCode = 31 * hashCode + charSequence.charAt(i);
            }

            return hashCode;
        }

    }
}