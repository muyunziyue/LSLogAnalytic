package com.bfd.etl.util;

import java.util.HashMap;
import java.util.Map;

/**
 * @ClassName ParseDataUtil
 * @Description TODO
 * @Author Lidexiu
 * @Date 2018/8/16 17:16
 * @Version 1.0
 **/
public class ParseDataUtil {


    public static Map<String, String> parseData(String data) {

        Map<String, String> map = new HashMap<>();
        String[] split = data.split("\\^A");
//        String remote_ip = split[0];
//        String msec = split[1];
//        String http_host = split[2];
        map.put("remote_ip", split[0]);
        map.put("msec", split[1]);
        map.put("http_host", split[2]);

        String request_uri = split[3];
        String[] split1_uri = request_uri.split("&");
        for (String uri : split1_uri) {
            String[] k_v = uri.split("=");
            map.put(k_v[0], k_v[1]);

        }

        return map;
    }
}
