package org.cloudcomputing.enums;

public enum GPACategory {
    FirstClass,
    SecondClassUpper,
    SecondClassLower,
    Normal;

    public static GPACategory fromGPA(double gpa) {
        if (gpa > 3.7) {
            return FirstClass;
        } else if (gpa > 3.5) {
            return SecondClassUpper;
        } else if (gpa >= 3.0) {
            return SecondClassLower;
        } else {
            return Normal;
        }
    }
}
