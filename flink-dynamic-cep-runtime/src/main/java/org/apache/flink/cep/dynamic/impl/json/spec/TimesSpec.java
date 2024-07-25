package org.apache.flink.cep.dynamic.impl.json.spec;


import org.apache.flink.cep.pattern.Quantifier;
import org.apache.flink.streaming.api.windowing.time.Time;

import javax.annotation.Nullable;
import java.util.concurrent.TimeUnit;

/**
 * @author shirukai
 */
public class TimesSpec {
    private int from;
    private int to;
    private @Nullable TimeSpec windowTime;

    public TimesSpec() {
    }

    public TimesSpec(int from, int to, @Nullable TimeSpec windowTime) {
        this.from = from;
        this.to = to;
        this.windowTime = windowTime;
    }

    public static TimesSpec of(Quantifier.Times times) {
        return new TimesSpec(times.getFrom(), times.getTo(), times.getWindowTime() == null ? null : TimeSpec.of(times.getWindowTime()));
    }

    public Quantifier.Times toTimes() {
        return Quantifier.Times.of(from, to, windowTime == null ? null : windowTime.toTime());
    }

    public int getFrom() {
        return from;
    }

    public void setFrom(int from) {
        this.from = from;
    }

    public int getTo() {
        return to;
    }

    public void setTo(int to) {
        this.to = to;
    }


    @Nullable
    public TimeSpec getWindowTime() {
        return windowTime;
    }

    public void setWindowTime(@Nullable TimeSpec windowTime) {
        this.windowTime = windowTime;
    }

    public static class TimeSpec {
        /**
         * The time unit for this policy's time interval.
         */
        private TimeUnit unit;

        /**
         * The size of the windows generated by this policy.
         */
        private long size;

        public TimeSpec() {
        }

        public TimeSpec(TimeUnit unit, long size) {
            this.unit = unit;
            this.size = size;
        }


        public static TimeSpec of(Time windowTime) {
            return new TimeSpec(windowTime.getUnit(), windowTime.getSize());
        }

        public Time toTime() {
            return Time.of(size, unit);
        }

        public TimeUnit getUnit() {
            return unit;
        }

        public void setUnit(TimeUnit unit) {
            this.unit = unit;
        }

        public long getSize() {
            return size;
        }

        public void setSize(long size) {
            this.size = size;
        }
    }

}
