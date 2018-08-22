package com.bfd.common;

/**
 * @ClassName KpiType
 * @Description TODO kpi 的枚举
 * @Author Lidexiu
 * @Date 2018/8/20 20:05
 * @Version 1.0
 **/
public enum  KpiType {
    NEW_USER("new_user"),
    BROWSER_NEW_USER("browser_new_user");

    public String kpiName;

    KpiType(String kpiName) {
        this.kpiName = kpiName;
    }

    public static KpiType valueOfKpiName(String kpiName) {
        for (KpiType kpi : values()) {
            if (kpi.kpiName.equals(kpiName)){
                return kpi;
            }
        }

        return null;
    }
}
