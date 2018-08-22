package com.bfd.common;

/**
 * @ClassName GlobalConstants
 * @Description  全局常量
 * @Author Lidexiu
 * @Date 2018/8/17 15:14
 * @Version 1.0
 **/
public class GlobalConstants {

    public static final String DRIVRE = "com.mysql.jdbc.Driver";
    public static final String URL = "jdbc:mysql://hadoop01:3306/report";
    public static final String USERNAME = "root";
    public static final String PASSWORD = "root";

    public static final String RUNNING_DATE = "running_date";
    public static final String DEFAULT_VALUE = "unknown";
    public static final String ALL_OF_VALUE = "all";
    public static final String WRITER_PREFIX = "writer_";
    public static final int BATCH_NUMBER = 50;
    public static final long DAY_OF_MILLSECONDS = 24 * 60 * 60 * 1000L;
}
