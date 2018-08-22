package com.bfd.util;

import com.bfd.common.DateEnum;
import org.apache.commons.lang.StringUtils;

import java.text.ParseException;
import java.text.SimpleDateFormat;
import java.util.Calendar;
import java.util.Date;
import java.util.regex.Matcher;
import java.util.regex.Pattern;

/**
 * @ClassName TimeUtil
 * @Description 全局的时间工具类
 * @Author Lidexiu
 * @Date 2018/8/17 11:12
 * @Version 1.0
 **/
public class TimeUtil {
    private static final String DFFAULT_FORMAT = "yyyy-MM-dd";


    /**
     * 判断时间是否有效
     *
     * @param date //
     * @return
     */
    public static boolean isValidDate(String date) {
        Matcher matcher = null;
        Boolean res = false;
        // TODO 判断时间的格式是否正确
        String regex = "[0-9]{4}-[0-9]{1,2}-[0-9]{1,2}";
        if (StringUtils.isNotEmpty(date)) {
            Pattern pattern = Pattern.compile(regex);
            matcher = pattern.matcher(date);
        }
        if (matcher != null) {
            res = matcher.matches();
        }

        return res;
    }


    /**
     * 默认获取昨天的日期 yyyy-MM-dd
     *
     * @return
     */
    public static String getYesterday() {

        return getYesterday(DFFAULT_FORMAT);
    }

    /**
     * 获取指定格式的昨天的日期
     *
     * @param pattern
     * @return
     */
    public static String getYesterday(String pattern) {
        SimpleDateFormat sdf = new SimpleDateFormat(pattern);
        Calendar calendar = Calendar.getInstance();
        calendar.add(Calendar.DAY_OF_YEAR, -1);
        return sdf.format(calendar.getTime());
    }

    /**
     * 将时间戳转换成默认格式的日期
     *
     * @param time
     * @return
     */
    public static String parseLong2String(long time) {

        return parseLong2String(time, DFFAULT_FORMAT);
    }

    /**
     * 将时间戳转换成指定格式的日期
     *
     * @param time
     * @param pattern
     * @return
     */
    public static String parseLong2String(long time, String pattern) {

        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        return new SimpleDateFormat(pattern).format(calendar.getTime());
    }

    /**
     * 将日期转换成默认格式的时间戳
     *
     * @param date
     * @return
     */
    public static long parseString2long(String date) {

        return parseString2long(date, DFFAULT_FORMAT);
    }

    /**
     * 将日期转换成指定格式的时间戳
     *
     * @param date
     * @param pattern
     * @return
     */
    public static long parseString2long(String date, String pattern) {

        Date dt = null;
        try {
            dt = new SimpleDateFormat(pattern).parse(date);
        } catch (ParseException e) {
            e.printStackTrace();
        }
        return dt.getTime();
    }

    /**
     * 根据时间戳和type来获取相对应的值
     * @param time
     * @param type
     * @return
     */
    public static int getDateInfo(Long time, DateEnum type) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        if(DateEnum.YEAR.equals(type)){
            return calendar.get(Calendar.YEAR);
        }
        if(DateEnum.SEASON.equals(type)){
            int month = calendar.get(Calendar.MONTH) + 1;
            // 123  1  ； 456  2 ； 789 3 ；10,11，12 4
            if (month % 3 == 0){
                return month / 3;
            }
            return month / 3 + 1;
        }

        if(DateEnum.MONTH.equals(type)){
            int month = calendar.get(Calendar.MONTH) + 1;
            return month;
        }

        if(DateEnum.WEEK.equals(type)){
            return calendar.get(Calendar.WEEK_OF_YEAR);
        }

        if(DateEnum.DAY.equals(type)){
            return calendar.get(Calendar.DAY_OF_MONTH);
        }

        if(DateEnum.HOUR.equals(type)){
            return calendar.get(Calendar.HOUR_OF_DAY);
        }
        throw new RuntimeException("该类型暂不支持时间信息获取.type:"+type.typeName);
    }

    public static long getFirstDayOfWeek(Long time) {
        Calendar calendar = Calendar.getInstance();
        calendar.setTimeInMillis(time);
        calendar.set(Calendar.DAY_OF_WEEK,1);
        calendar.set(Calendar.HOUR_OF_DAY,0);
        calendar.set(Calendar.MINUTE,0);
        calendar.set(Calendar.SECOND,0);
        calendar.set(Calendar.MILLISECOND,0);
        return calendar.getTimeInMillis();
    }


    public static void main(String[] args) {
        System.out.println(getYesterday("yyyy/MM/dd"));
        System.out.println(getYesterday());
        System.out.println(isValidDate("2018-06-32"));
        System.out.println(parseString2long("2018-08-17"));
        System.out.println(parseLong2String(1530720000000l));
    }

}
