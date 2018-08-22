package com.bfd.common;

public enum DateEnum {

    YEAR("year"),
    SEASON("season"),
    MONTH("month"),
    WEEK("week"),
    DAY("day"),
    HOUR("hour");

    public final String typeName;

    DateEnum(String typeName) {
        this.typeName = typeName;
    }

}
