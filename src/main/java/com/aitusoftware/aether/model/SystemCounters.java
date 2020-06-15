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
package com.aitusoftware.aether.model;

/**
 * Models counters associated with a MediaDriver.
 */
public final class SystemCounters
{
    private long bytesSent;
    private long bytesReceived;
    private long naksSent;
    private long naksReceived;
    private long errors;
    private long clientTimeouts;

    public long bytesSent()
    {
        return bytesSent;
    }

    public void bytesSent(final long bytesSent)
    {
        this.bytesSent = bytesSent;
    }

    public long bytesReceived()
    {
        return bytesReceived;
    }

    public void bytesReceived(final long bytesReceived)
    {
        this.bytesReceived = bytesReceived;
    }

    public long naksSent()
    {
        return naksSent;
    }

    public void naksSent(final long naksSent)
    {
        this.naksSent = naksSent;
    }

    public long naksReceived()
    {
        return naksReceived;
    }

    public void naksReceived(final long naksReceived)
    {
        this.naksReceived = naksReceived;
    }

    public long errors()
    {
        return errors;
    }

    public void errors(final long errors)
    {
        this.errors = errors;
    }

    public long clientTimeouts()
    {
        return clientTimeouts;
    }

    public void clientTimeouts(final long clientTimeouts)
    {
        this.clientTimeouts = clientTimeouts;
    }

    public void copyInto(final SystemCounters systemCounters)
    {
        systemCounters.bytesSent = bytesSent;
        systemCounters.bytesReceived = bytesReceived;
        systemCounters.naksSent = naksSent;
        systemCounters.naksReceived = naksReceived;
        systemCounters.errors = errors;
        systemCounters.clientTimeouts = clientTimeouts;
    }

    @Override
    public String toString()
    {
        return "SystemCounters{" +
            "bytesSent=" + bytesSent +
            ", bytesReceived=" + bytesReceived +
            ", naksSent=" + naksSent +
            ", naksReceived=" + naksReceived +
            ", errors=" + errors +
            ", clientTimeouts=" + clientTimeouts +
            '}';
    }
}