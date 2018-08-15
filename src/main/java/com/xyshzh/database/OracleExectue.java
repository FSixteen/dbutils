package com.xyshzh.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * Oracle 数据库.
 * 
 * @author Shengjun Liu
 * @date 2018-08-15
 */
public class OracleExectue extends DatabaseExecute {

  public static enum Type {
    SID, SERVER
  }

  private static final long serialVersionUID = -6617807679829663071L;

  private transient String ip = null;
  private transient String port = null;
  private transient String database = null;
  private transient String user = null;
  private transient String password = null;
  private String driverClass = "oracle.jdbc.driver.OracleDriver";
  private Type type = null;
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
  public OracleExectue(String ip, String port, String database, String user, String password) {
    this(ip, port, database, user, password, Type.SERVER);
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
   * @param type
   *          数据库链接方式, SID | SERVER.
   */
  public OracleExectue(String ip, String port, String database, String user, String password, Type type) {
    this.ip = ip;
    this.port = port;
    this.database = database;
    this.user = user;
    this.password = password;
    this.type = type;
  }

  @Override
  public Connection getConnection() {
    try {
      if (null == con || con.isClosed()) {
        int time = 0;
        String url = null;
        if (Type.SID == type) {
          url = "jdbc:oracle:thin:@" + this.ip + ":" + this.port + ":" + this.database;
        } else {
          url = "jdbc:oracle:thin:@//" + this.ip + ":" + this.port + "/" + this.database;
        }
        try {
          for (; (++time < 3) && (null == con || con.isClosed());) {
            Class.forName(getDriverClass());
            con = DriverManager.getConnection(url, user, password);
            System.out.println("数据库链接获取成功.");
          }
        } catch (Exception e) {
          System.err.println("数据库链接获取失败.");
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
