/**
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package com.victoriametrics.presto.util

import java.util.concurrent.TimeUnit

class Stopwatch private constructor(private val ticker: Ticker) {

    companion object {
        /**
         * Default ticker for measuring relative time spans.
         * @see System.nanoTime
         */
        @JvmStatic
        fun runtime() =
            Stopwatch(RuntimeTicker)

        /**
         * Wall-clock ticker.
         * @see System.currentTimeMillis
         */
        @JvmStatic
        fun wallclock() =
            Stopwatch(WallclockTicker)
    }

    private interface Ticker {
        fun read(): Long
    }

    private object RuntimeTicker : Ticker {
        override fun read() = System.nanoTime()
    }

    private object WallclockTicker : Ticker {
        override fun read(): Long {
            return System.currentTimeMillis() * 1000_000
        }
    }

    private var isRunning: Boolean = false
    private var elapsedNanos: Long = 0
    private var startTick: Long = 0

    constructor() : this(RuntimeTicker)

    init {
        start()
    }

    private fun elapsed(timeUnit: TimeUnit): Long {
        return timeUnit.convert(elapsedNanos(), TimeUnit.NANOSECONDS)
    }

    private fun elapsedNanos(): Long {
        return if (isRunning) {
            ticker.read() - startTick + elapsedNanos
        } else {
            elapsedNanos
        }
    }

    fun h() = elapsed(TimeUnit.HOURS)

    fun m() = elapsed(TimeUnit.MINUTES)

    fun s() = elapsed(TimeUnit.SECONDS)

    fun ms() = elapsed(TimeUnit.MILLISECONDS)

    fun mk() = elapsed(TimeUnit.MICROSECONDS)

    fun ns() = elapsed(TimeUnit.NANOSECONDS)

    fun start(): Stopwatch {
        check(!isRunning, { "This stopwatch is already running." })
        isRunning = true
        startTick = ticker.read()
        return this
    }

    fun stop(): Stopwatch {
        val tick = ticker.read()
        check(isRunning) { "This stopwatch is already stopped." }
        isRunning = false
        elapsedNanos += tick - startTick
        return this
    }

    fun reset(): Stopwatch {
        elapsedNanos = 0
        isRunning = false
        return this
    }

    fun restart(): Stopwatch {
        reset()
        start()
        return this
    }
}
