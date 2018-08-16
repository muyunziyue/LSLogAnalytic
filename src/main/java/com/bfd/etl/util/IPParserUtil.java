package com.bfd.etl.util;


import com.bfd.etl.util.ip.IPSeeker;
import cz.mallat.uasparser.UASparser;

import org.apache.hadoop.hbase.protobuf.generated.HBaseProtos;
import org.apache.log4j.Logger;

import javax.swing.plaf.synth.Region;

/**
 * @ClassName IPParserUtil
 * @Description TODO ip解析工具类
 * @Author Lidexiu
 * @Date 2018/8/15 16:02
 * @Version 1.0
 **/
public class IPParserUtil extends IPSeeker {
    private static Logger logger = Logger.getLogger(IPParserUtil.class);

    RegionInfo info = new RegionInfo();

    public RegionInfo parserIp(String ip) {
        if(ip.isEmpty()) {
            logger.warn("Ip may be null");
            return info;
        }

        // 取出数据的样式: 浙江省杭州市 || 局域网
        String country = IPSeeker.getInstance().getCountry(ip);
        if(country.isEmpty()) {
            logger.warn("查无此ip信息,或者ip地址格式错误: " + ip);
            return info;
        }
        if(country.equals("局域网")) {
            info.setCountry("中国");
            info.setProvince("北京");
            info.setCity("昌平区");
        } else if (country != null && !country.trim().isEmpty()) {
            info.setCountry("中国");
            int index = country.indexOf("省");
            if(index >= 0) {
                info.setProvince(country.substring(0, index + 1));
                int index2 = country.indexOf("市");
                if (index2 > 0) {
                    info.setCity(country.substring(index + 1, index2 +1));
                }
            }else {
                String flag = country.substring(0, 2);
                String sub = null;
                switch (flag){
                    case "内蒙":
                        // 设置省份
                        info.setProvince(flag + "古");
                        sub = country.substring(3);
                        index = sub.indexOf("市");
                        if(index > 0) {
                            info.setCity(sub.substring(0, index + 1));
                        }
                        break;
                    case "新疆":
                    case "西藏":
                    case "广西":
                    case "宁夏":
                        // 设置省份
                        info.setProvince(flag);
                        sub = country.substring(2);
                        index = sub.indexOf("市");
                        if(index > 0) {
                            info.setCity(sub.substring(0, index + 1));
                        }
                        break;

                    case "北京":
                    case "天津":
                    case "上海":
                    case "重庆":
                        info.setProvince(flag + "市");
                        sub = country.substring(2);
                        index = sub.indexOf("区");
                        if (index > 0) {
                            char ch = sub.charAt(index - 1);
                            if(ch != '小' || ch != '军' || ch != '校') {
                                info.setCity(sub.substring(0, index + 1));
                            }
                        }
                        index = sub.indexOf("县");
                        if (index > 0) {
                            info.setCity(sub.substring(0, index + 1));
                        }
                        break;

                    case "香港":
                    case "澳门":
                    case "台湾":
                        info.setProvince(flag + "特别行政区");
                        break;
                    default:
                        break;
                }
            }

        }

        return info;

    }

    /**
     * 用于封装ip解析出来的国家,省,市信息
     */
    public static class RegionInfo{
        private static final String DEFAULT_VALUE = "unknow";
        private String country = DEFAULT_VALUE;
        private String province = DEFAULT_VALUE;
        private String city = DEFAULT_VALUE;

        public RegionInfo() {}
        public RegionInfo(String country, String province, String city) {
            this. country = country;
            this.province = province;
            this.city = city;
        }

        public String getCountry() {
            return country;
        }

        public void setCountry(String country) {
            this.country = country;
        }

        public String getProvince() {
            return province;
        }

        public void setProvince(String province) {
            this.province = province;
        }

        public String getCity() {
            return city;
        }

        public void setCity(String city) {
            this.city = city;
        }

        @Override
        public String toString() {
            return "RegionInfo{" +
                    "country='" + country + '\'' +
                    ", province='" + province + '\'' +
                    ", city='" + city + '\'' +
                    '}';
        }
    }

}
