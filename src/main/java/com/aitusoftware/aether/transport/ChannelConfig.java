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

final class ChannelConfig
{
    private static final String AETHER_CHANNEL_DEFAULT = "aeron:udp?endpoint=localhost:15566";
    private static final String AETHER_TRANSPORT_CHANNEL_PROP_NAME = "aether.transport.channel";
    private static final String AETHER_TRANSPORT_STREAM_ID_PROP_NAME = "aether.transport.streamId";
    static final String AETHER_CHANNEL = System.getProperty(AETHER_TRANSPORT_CHANNEL_PROP_NAME,
        AETHER_CHANNEL_DEFAULT);
    static final int AETHER_STREAM_ID = Integer.getInteger(AETHER_TRANSPORT_STREAM_ID_PROP_NAME, 0xAE01);
}