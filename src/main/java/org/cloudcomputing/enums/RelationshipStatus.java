package org.cloudcomputing.enums;

public enum RelationshipStatus {
    InRelationship, NoRelationship;

    public static RelationshipStatus fromBoolean(boolean in) {
        return in ? InRelationship : NoRelationship;
    }
}
