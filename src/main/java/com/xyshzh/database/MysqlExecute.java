package com.xyshzh.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Mysql 数据库.
 * 
 * @author Shengjun Liu
 * @date 2018-08-15
 */
public class MysqlExecute extends DatabaseExecute {

  private static final long serialVersionUID = 4769587399306040595L;
  public static final String V56 = "com.mysql.jdbc.Driver";
  public static final String V57 = "com.mysql.cj.jdbc.Driver";

  private transient String ip = null;
  private transient String port = null;
  private transient String database = null;
  private transient String user = null;
  private transient String password = null;
  private transient String driverClass = null;
  private Connection con = null;

  /**
   * 构造方法.
   * 
   * @param ip
   *          IP
   * @param port
   *          Port
   * @param database
   *          数据库名称.
   * @param user
   *          用户名称.
   * @param password
   *          用户密码.
   */
  public MysqlExecute(String ip, String port, String database, String user, String password) {
    this(ip, port, database, user, password, V56);
  }

  /**
   * 构造方法.
   * 
   * @param ip
   *          IP
   * @param port
   *          Port
   * @param database
   *          数据库名称.
   * @param user
   *          用户名称.
   * @param password
   *          用户密码.
   * @param driverClass
   *          驱动名称.
   */
  public MysqlExecute(String ip, String port, String database, String user, String password, String driverClass) {
    this.ip = ip;
    this.port = port;
    this.database = database;
    this.user = user;
    this.password = password;
    this.driverClass = driverClass;
  }

  @Override
  public Connection getConnection() {
    try {
      if (null == con || con.isClosed()) {
        int time = 0;
        String url = "jdbc:mysql://" + this.ip + ":" + this.port + "/" + this.database
            + "?useUnicode=true&characterEncoding=UTF-8&autoReconnect=true&zeroDateTimeBehavior=convertToNull&serverTimezone=Asia/Shanghai&useSSL=false";
        try {
          Class.forName(getDriverClass());
          for (; (++time < 3) && (null == this.con || this.con.isClosed());) {
            con = DriverManager.getConnection(url, user, password);
            System.out.println("数据库链接获取成功.");
          }
        } catch (Exception e) {
          System.out.println("数据库链接获取失败.");
          e.printStackTrace();
        }
      }
    } catch (SQLException e) {
      System.err.println("数据库链接获取失败.");
      e.printStackTrace();
    }
    return con;
  }

  @Override
  public String getDriverClass() {
    return this.driverClass;
  }

}
