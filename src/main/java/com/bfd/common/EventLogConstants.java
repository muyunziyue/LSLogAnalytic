package com.bfd.common;

/**
 * @ClassName EventLogConstants
 * @Description TODO 日志的常量类
 * @Author Lidexiu
 * @Date 2018/8/17 9:48
 * @Version 1.0
 **/
public class EventLogConstants {


    /**
     * 事件的枚举
     */
    public static enum EventEnum{
        LAUNCH(1, "launch_event", "e_l"),
        PAGEVIEW(2, "page_view", "e_pv"),
        CHARGERQUEST(3, "charge_request", "e_crt"),
        CHARGESUCCESS(4, "charge_success", "e_sc"),
        CHARGEREFUND(5, "charge_refund", "e_cr"),
        EVENT(6, "event", "e_e"),
        ;
        public final int id; // 事件的id
        public final String name;
        public final String alias; // 别名

        // 构造器
        EventEnum(int id, String name, String alias) {
            this.id = id;
            this.name = name;
            this.alias = alias;
        }

        /**
         * 根据别名获取枚举
         * @return
         */
        public static EventEnum valueOfAlias(String alias) {
            for (EventEnum event : values()){ // 返回整个枚举
                if (alias.equals(event.alias)){ // 传过来的alias如果存在则返回
                    return event;
                }
            }

            return null;
        }
    }
    /**
     * hbase表相关
     */
    public static String HBASE_TABLE_NAME = "logs";

    public static final String EVENT_LOG_FAMILY_NAME = "info";

    public static final String Event_LOG_SEPATOR = "\\^A";


    public static final String EVENT_COLUMN_NAME_VERSION = "ver";

    public static final String EVENT_COLUMN_NAME_SERVER_TIME = "s_time";

    public static final String EVENT_COLUMN_NAME_EVENT_NAME = "en";

    public static final String EVENT_COLUMN_NAME_UUID = "u_ud";

    public static final String EVENT_COLUMN_NAME_MEMBER_ID = "u_mid";

    public static final String EVENT_COLUMN_NAME_SESSION_ID = "u_sd";

    public static final String EVENT_COLUMN_NAME_CLIENT_TIME = "c_time";

    public static final String EVENT_COLUMN_NAME_LANGUAGE = "l";

    public static final String EVENT_COLUMN_NAME_USERAGENT = "b_iev";

    public static final String EVENT_COLUMN_NAME_RESOLUTION = "b_rst";

    public static final String EVENT_COLUMN_NAME_CURRENT_URL = "p_url";

    public static final String EVENT_COLUMN_NAME_PREFFER_URL = "p_ref";

    public static final String EVENT_COLUMN_NAME_TITLE = "tt";

    public static final String EVENT_COLUMN_NAME_PLATFORM = "pl";

    public static final String EVENT_COLUMN_NAME_IP = "ip";


    /**
     * 和订单相关
     */
    public static final String EVENT_COLUMN_NAME_ORDER_ID = "oid";

    public static final String EVENT_COLUMN_NAME_ORDER_NAME = "on";

    public static final String EVENT_COLUMN_NAME_CURRENCY_AMOUNT = "cua";

    public static final String EVENT_COLUMN_NAME_CURRENCY_TYPE = "cut";

    public static final String EVENT_COLUMN_NAME_PAYMENT_TYPE = "pt";


    /**
     * 事件相关
     * 点击：点击事件 category   点赞、收藏、喜欢、转发
     * 下单：下单事件
     */
    public static final String EVENT_COLUMN_NAME_EVENT_CATEGORY = "ca";

    public static final String EVENT_COLUMN_NAME_EVENT_ACTION = "ac";

    public static final String EVENT_COLUMN_NAME_EVENT_KV = "kv_";

    public static final String EVENT_COLUMN_NAME_EVENT_DURATION = "du"; // 事件持续时间

    /**
     * 浏览器相关
     */

    public static final String EVENT_COLUMN_NAME_BROWSER_NAME = "browserName";

    public static final String EVENT_COLUMN_NAME_BROWSER_VERSION = "browserVersion";

    public static final String EVENT_COLUMN_NAME_OS_NAME = "osName";

    public static final String EVENT_COLUMN_NAME_OS_VERSION = "osVersion";

    /**
     * 地域相关
     */

    public static final String EVENT_COLUMN_NAME_COUNTRY = "country";

    public static final String EVENT_COLUMN_NAME_PROVINCE = "province";

    public static final String EVENT_COLUMN_NAME_CITY = "city";

}
