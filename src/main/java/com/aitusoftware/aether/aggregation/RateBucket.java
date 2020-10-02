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
package com.aitusoftware.aether.aggregation;

import java.util.concurrent.TimeUnit;

public final class RateBucket
{
    private final long duration;
    private final TimeUnit durationUnit;

    public RateBucket(final long duration, final TimeUnit durationUnit)
    {
        this.duration = duration;
        this.durationUnit = durationUnit;
    }

    public long getDuration()
    {
        return duration;
    }

    public TimeUnit getDurationUnit()
    {
        return durationUnit;
    }
}