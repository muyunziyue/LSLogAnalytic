package com.bfd.analystic.mr.service.impl;

import com.bfd.analystic.model.dim.base.*;
import com.bfd.analystic.mr.service.IDimensionConvert;
import com.bfd.util.jdbcUtil;
import org.apache.log4j.Logger;

import java.sql.*;
import java.util.LinkedHashMap;
import java.util.Map;

/**
 * @ClassName IDimensionConvertImpl
 * @Description TODO 根据维度获取维度id的接口的实现
 * @Author Lidexiu
 * @Date 2018/8/21 9:36
 * @Version 1.0
 **/
public class IDimensionConvertImpl implements IDimensionConvert {

    private static final Logger logger = Logger.getLogger(IDimensionConvertImpl.class);
    // <String, Integer> --> 维度:维度所对应的id
    private Map<String, Integer> cache = new LinkedHashMap<String, Integer>() {
        @Override
        protected boolean removeEldestEntry(Map.Entry<String, Integer> eldest) {
            return this.size() > 1000; // 根据自己的内存判定,此处是当大于1000个是自动去除存在最久的元素
        }
    };

    /**
     * 获取对应维度的id
     *
     * @param dimension
     * @return
     */
    @Override
    public int getDimensionIdByValue(BaseDimension dimension) {
        String cacheKey = null;
        Connection conn = null;
        String[] sqls = new String[0];
        try {
            // 申请的维度的缓存key
            cacheKey = buildCacheKey(dimension);
            if (this.cache.containsKey(cacheKey)) {
                return this.cache.get(cacheKey);
            }
            // 代码走到这代表cache中没有对应的维度
            // 去mysql中查找, 如有则返回id, 如没有先插入在返回对应的维度id
            conn = jdbcUtil.getConn();
            sqls = null;
            if (dimension instanceof PlatformDimension) {
                sqls = buildPlatformSqls(dimension);
            } else if (dimension instanceof KpiDimension) {
                sqls = buildKpiSqls(dimension);
            } else if (dimension instanceof DateDimension) {
                sqls = buildDateSqls(dimension);
            } else if (dimension instanceof BrowserDimension) {
                sqls = buildBrowserSqls(dimension);
            }
            // 执行sql
            int id = -1;
            synchronized (this) {
                id = this.executeSqls(sqls, dimension, conn);
            }

            // 将获取的id放到缓存中
            this.cache.put(cacheKey, id);
            return id;
//            } else {
//                // 抛异常
//            }
        } catch (Exception e) {
            logger.warn("获取维度id异常", e);
        }
        throw new RuntimeException("获取维度id运行异常");

    }

    /**
     * 构建缓存key,
     *
     * @param dimension
     * @return
     */
    private String buildCacheKey(BaseDimension dimension) {
        StringBuffer sb = new StringBuffer();

        if (dimension instanceof PlatformDimension) {
            PlatformDimension platform = (PlatformDimension) dimension;
            sb.append("platform_");
            sb.append(platform.getPlatformNmae());
        } else if (dimension instanceof KpiDimension) {
            KpiDimension kpi = (KpiDimension) dimension;
            sb.append("kpi_");
            sb.append(kpi.getKpiName());
        } else if (dimension instanceof BrowserDimension) {
            BrowserDimension browser = (BrowserDimension) dimension;
            sb.append("browser_");
            sb.append(browser.getBrowserName());
            sb.append(browser.getBrowserVersion());
        } else if (dimension instanceof DateDimension) {
            DateDimension date = (DateDimension) dimension;
            sb.append("date_");
            sb.append(date.getYear());
            sb.append(date.getSeason());
            sb.append(date.getMonth());
            sb.append(date.getWeek());
            sb.append(date.getDay());
            sb.append(date.getType());
        }

        return sb.toString(); // 此处最好判断是否为空,在返回
    }

    /**
     * 第一个查询id的sql, 第二个插入sql
     *
     * @param dimension
     * @return
     */
    private String[] buildBrowserSqls(BaseDimension dimension) {
        String query = "select id from dimension_browser where browser_name = ? and browser_version = ?";
        String insert = "insert into dimension_platform(platform_name,platform_version) values(?,?)";
        return new String[]{query, insert};

    }

    private String[] buildDateSqls(BaseDimension dimension) {
        // `year``season``month``week``day``calendar``type`
        String query = "select id from dimension_date where year = ? and season = ? and month =? and week = ? and day = ? and calendar = ? and type = ?";
        String insert = "insert into dimension_date(`year`,`season`,`month`,`week`,`day`,`type`,`calendar`) values(?,?,?,?,?,?,?)";
        return new String[]{query, insert};
    }

    private String[] buildKpiSqls(BaseDimension dimension) {
        String query = "select id from dimension_kpi where kpi_name = ?";
        String insert = "insert into dimension_kpi(kpi_name) values(?)";
        return new String[]{query, insert};
    }

    private String[] buildPlatformSqls(BaseDimension dimension) {
        String query = "select id from dimension_platform where platform_name = ?";
        String insert = "insert into dimension_platform(platform_name) values(?)";
        return new String[]{query, insert};
    }

    /**
     * 执行sql
     *
     * @param sqls
     * @param dimension
     * @param conn
     * @return
     */
    private int executeSqls(String[] sqls, BaseDimension dimension, Connection conn) {
        PreparedStatement ps = null;
        ResultSet rs = null;

        // 先查询
        try {
            ps = conn.prepareStatement(sqls[0]);
            // 赋值
            this.setArgs(dimension, ps);
            // 执行
            rs = ps.executeQuery();
            if (rs.next()) {
                return rs.getInt(1);
            }
            // 代码走到这,代表没有查询到对应的id, 则插入并查询
            ps = conn.prepareStatement(sqls[1], Statement.RETURN_GENERATED_KEYS);
            this.setArgs(dimension, ps);
            ps.executeUpdate(); // TODO 执行sql报错
            rs = ps.getGeneratedKeys();
            if (rs.next()) {
                return rs.getInt(1);
            }

        } catch (SQLException e) {
            logger.warn("执行sql异常", e);
        }
        throw new RuntimeException("执行sql语句时运行异常");
    }

    /**
     * 设置参数
     *
     * @param dimension
     * @param ps
     */
    private void setArgs(BaseDimension dimension, PreparedStatement ps) {
        try {
            int i = 0;
            if (dimension instanceof PlatformDimension) {
                PlatformDimension platform = (PlatformDimension) dimension;
                ps.setString(++i, platform.getPlatformNmae());
            } else if (dimension instanceof KpiDimension) {
                KpiDimension kpi = (KpiDimension) dimension;
                ps.setString(++i, kpi.getKpiName());


            } else if (dimension instanceof BrowserDimension) {
                BrowserDimension browser = (BrowserDimension) dimension;
                ps.setString(++i, browser.getBrowserName());
                ps.setString(++i, browser.getBrowserVersion());
            } else if (dimension instanceof DateDimension) {
                DateDimension date = (DateDimension) dimension;
                ps.setInt(++i, date.getYear());
                ps.setInt(++i, date.getSeason());
                ps.setInt(++i, date.getMonth());
                ps.setInt(++i, date.getWeek());
                ps.setInt(++i, date.getDay());
                ps.setString(++i, date.getType());
                ps.setDate(++i, new Date(date.getCalendar().getTime()));
            }


        } catch (SQLException e) {
            e.printStackTrace();
        }

    }


}
