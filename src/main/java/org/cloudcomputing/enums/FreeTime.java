package org.cloudcomputing.enums;

public enum FreeTime {
    LowFreeTime,
    HighFreeTime;

    public static FreeTime fromFreeTime(int freeTime) {
        return (freeTime <= 2) ? LowFreeTime : HighFreeTime;
    }
}
