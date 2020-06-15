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

import java.util.Objects;

/**
 * Represents a single data stream, identified by channel and streamId.
 */
public final class StreamKey
{
    private final String channel;
    private final int streamId;

    StreamKey(final String channel, final int streamId)
    {
        this.channel = channel;
        this.streamId = streamId;
    }

    /**
     * Returns the channel.
     *
     * @return the channel
     */
    public String getChannel()
    {
        return channel;
    }

    /**
     * Returns the streamId.
     *
     * @return the streamId
     */
    public int getStreamId()
    {
        return streamId;
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
        final StreamKey streamKey = (StreamKey)o;
        return streamId == streamKey.streamId &&
            Objects.equals(channel, streamKey.channel);
    }

    @Override
    public int hashCode()
    {
        return Objects.hash(channel, streamId);
    }
}
