package com.bfd.etl.util;

import cz.mallat.uasparser.OnlineUpdater;
import cz.mallat.uasparser.UASparser;
import cz.mallat.uasparser.UserAgentInfo;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.IOException;

/**
 * @ClassName UserAgentUtil
 * @Description 解析userAgentUtil代理对象
 * @Author Lidexiu
 * @Date 2018/8/16 8:11
 * @Version 1.0
 **/
public class UserAgentUtil {
    private static Logger logger = Logger.getLogger(UserAgentUtil.class);

    private static UASparser ua = null;

    // 初始化
    static {
        try {
            ua = new UASparser(OnlineUpdater.getVendoredInputStream());
        } catch (IOException e) {
            e.printStackTrace();
        }
    }

    // 解析代理对象
    public static UserAgentInfo parseUserAgent(String agent) {
        if (StringUtils.isEmpty(agent)){
            logger.warn("agent may be null");
            return null;
        }
        UserAgentInfo info = new UserAgentInfo();

        try {
            cz.mallat.uasparser.UserAgentInfo parse = ua.parse(agent);
            info.setBrowserName(parse.getUaFamily());
            info.setBrowserVersion(parse.getBrowserVersionInfo());
            info.setOsName(parse.getOsFamily());
            info.setOsVersion(parse.getOsName());

            return info;
        } catch (IOException e) {
            e.printStackTrace();
        }

        return null;
    }

    // 用于封装解析出来的浏览器名,版本,操作系统名和版本
    public static class UserAgentInfo {
        private String browserName;
        private String browserVersion;
        private String osName;
        private String osVersion;

        public UserAgentInfo() {
        }

        @Override
        public String toString() {
            return "UserAgentInfo{" +
                    "browserName='" + browserName + '\'' +
                    ", browserVersion='" + browserVersion + '\'' +
                    ", osName='" + osName + '\'' +
                    ", osVersion='" + osVersion + '\'' +
                    '}';
        }

        public String getBrowserName() {
            return browserName;
        }

        public void setBrowserName(String browserName) {
            this.browserName = browserName;
        }

        public String getBrowserVersion() {
            return browserVersion;
        }

        public void setBrowserVersion(String browserVersion) {
            this.browserVersion = browserVersion;
        }

        public String getOsName() {
            return osName;
        }

        public void setOsName(String osName) {
            this.osName = osName;
        }

        public String getOsVersion() {
            return osVersion;
        }

        public void setOsVersion(String osVersion) {
            this.osVersion = osVersion;
        }
    }

}
