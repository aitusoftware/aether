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
package com.aitusoftware.aether.model;

import java.util.Objects;

/**
 * Describes a participant in a message flow.
 */
public final class ChannelSessionKey
{
    private final String label;
    private final String channel;
    private final int streamId;
    private final int sessionId;

    public ChannelSessionKey(
        final String label,
        final String channel,
        final int streamId,
        final int sessionId)
    {
        this.label = label;
        this.channel = channel;
        this.streamId = streamId;
        this.sessionId = sessionId;
    }

    public String getLabel()
    {
        return label;
    }

    public String getChannel()
    {
        return channel;
    }

    public int getStreamId()
    {
        return streamId;
    }

    public int getSessionId()
    {
        return sessionId;
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
        final ChannelSessionKey that = (ChannelSessionKey)o;
        return streamId == that.streamId &&
            sessionId == that.sessionId &&
            Objects.equals(channel, that.channel) &&
            Objects.equals(label, that.label);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(label, channel, streamId, sessionId);
    }
}