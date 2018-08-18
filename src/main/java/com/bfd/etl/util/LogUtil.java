package com.bfd.etl.util;

import com.bfd.common.EventLogConstants;
import org.apache.commons.lang.StringUtils;
import org.apache.log4j.Logger;

import java.io.UnsupportedEncodingException;
import java.net.URLDecoder;
import java.util.Map;
import java.util.concurrent.ConcurrentHashMap;

/**
 * @ClassName LogUtil
 * @Description TODO 整行日志的解析工具
 * @Author Lidexiu
 * @Date 2018/8/17 9:41
 * @Version 1.0
 **/
public class LogUtil {
    private static final Logger logger = Logger.getLogger(LogUtil.class);

    /**
     * 单行日志的解析
     * @param log 114.61.94.253^A1531110990.123^Ahh^A/BCImg.gif?en=e_l&ver=1&pl=website&sdk=js&u_ud=27F69684-BBE3-42FA-AA62-71F98E208444&u_mid=Aidon&u_sd=38F66FBB-C6D6-4C1C-8E05-72C31675C00A&c_time=1449917532123&l=zh-CN&b_iev=Mozilla%2F5.0%20(Windows%20NT%206.1%3B%20WOW64)%20AppleWebKit%2F537.36%20(KHTML%2C%20like%20Gecko)%20Chrome%2F46.0.2490.71%20Safari%2F537.36&b_rst=1280*768
     * @return
     */
    public static Map<String, String> handleLog(String log) {

        // 线程安全
        Map<String, String> info = new ConcurrentHashMap<>();
        if (StringUtils.isNotEmpty(log.trim())){
            // 拆分单行日志
            String[] fields = log.split(EventLogConstants.Event_LOG_SEPATOR);

            if (fields.length == 4) {
                //
                info.put(EventLogConstants.EVENT_COLUMN_NAME_IP, fields[0]);
                //TODO 此处处理有问题,已解决,忘记转义
                info.put(EventLogConstants.EVENT_COLUMN_NAME_SERVER_TIME, fields[1].replaceAll("\\.", ""));
                // 处理参数列表
                handleParams(info, fields[3]);
                // 处理ip
                handleIp(info);
                // 处理useragent
                hadleUserAgent(info);

            }
        }

        return  info;

    }

    /**
     * 处理ip
     * @param info
     */
    private static void handleIp(Map<String, String> info) {
        if (info.containsKey(EventLogConstants.EVENT_COLUMN_NAME_IP)) {
            IPParserUtil.RegionInfo ri = new IPParserUtil().parserIp(info.get(EventLogConstants.EVENT_COLUMN_NAME_IP));
            if (ri != null) {
                info.put(EventLogConstants.EVENT_COLUMN_NAME_COUNTRY, ri.getCountry());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_PROVINCE, ri.getProvince());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_CITY, ri.getCity());

            }
        }
    }

    /**
     * 处理agent
     * @param info
     */
    private static void hadleUserAgent(Map<String, String> info) {
        if (info.containsKey(EventLogConstants.EVENT_COLUMN_NAME_USERAGENT)) {
            UserAgentUtil.UserAgentInfo ua = UserAgentUtil.parseUserAgent(info.get(EventLogConstants.EVENT_COLUMN_NAME_USERAGENT));
            if (ua != null) {
                info.put(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_NAME, ua.getBrowserName());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_BROWSER_VERSION, ua.getBrowserVersion());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_OS_NAME, ua.getOsName());
                info.put(EventLogConstants.EVENT_COLUMN_NAME_OS_VERSION, ua.getOsVersion());
            }
        }
    }

    /**
     * 处理参数
     * @param info
     * @param field
     */
    private static void handleParams(Map<String, String> info, String field) {
        if (StringUtils.isNotEmpty(field)){
            int index = field.indexOf("?");
            if (index > 0) {
                String fields = field.substring(index + 1);
                String[] params = fields.split("&");
                for (String param :
                        params) {
                    String [] kvs = param.split("=");
                    try {
                        String k = kvs[0];
                        String v = URLDecoder.decode(kvs[1], "utf-8");
                        if (StringUtils.isNotEmpty(k)) {
                            // 存储数据到info
                            info.put(k, v);
                        }

                    } catch (UnsupportedEncodingException e) {
                        logger.warn("url的解码异常", e);
                    }
                }

            }
        }
    }
}
