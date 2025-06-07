package org.cloudcomputing.enums;

public enum PartTimeJob {
    WithPartTimeJob,
    NoPartTimeJob;

    public static PartTimeJob fromBoolean(boolean hasJob) {
        return hasJob ? WithPartTimeJob : NoPartTimeJob;
    }
}
