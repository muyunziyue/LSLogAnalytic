package com.bfd.util;

import com.bfd.common.GlobalConstants;

import java.sql.*;

/**
 * @ClassName jdbcUtil
 * @Description TODO 获取和关闭数据库的连接
 * @Author Lidexiu
 * @Date 2018/8/21 9:37
 * @Version 1.0
 **/
public class jdbcUtil {

    // 静态加载驱动
    static {
        try {
            Class.forName(GlobalConstants.DRIVRE);
        } catch (ClassNotFoundException e) {
            e.printStackTrace();
        }
    }

    // 获取连接
    public static Connection getConn(){
        Connection conn = null;

        try {
            conn = DriverManager.getConnection(GlobalConstants.URL, GlobalConstants.USERNAME, GlobalConstants.PASSWORD);
        } catch (SQLException e) {
            e.printStackTrace();
        }

        return conn;
    }

    /**
     *  关闭mysql的相关对象
     * @param conn
     * @param ps
     * @param rs
     */
    public static void close(Connection conn, PreparedStatement ps, ResultSet rs){
        if (conn != null){
            try {
                conn.close();
            } catch (SQLException e) {
                // do nothing
            }
        }
        if (ps != null){
            try {
                ps.close();
            } catch (SQLException e) {
                // do nothing
            }
        }
        if (rs != null){
            try {
                rs.close();
            } catch (SQLException e) {
                // do nothing
            }
        }

    }
}
