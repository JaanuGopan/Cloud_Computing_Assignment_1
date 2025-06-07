package org.cloudcomputing.enums;

public enum InternetStatus {
    HasInternetAccess, NoInternetAccess;

    public static InternetStatus fromBoolean(boolean hasInternetAccess) {
        return hasInternetAccess ? HasInternetAccess : NoInternetAccess;
    }
}
