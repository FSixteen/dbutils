package com.xyshzh.database;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.SQLException;

/**
 * SQL Server 数据库.
 * 
 * @author Shengjun Liu
 * @date 2018-08-15
 */
public class SqlServerExectue extends DatabaseExecute {

  private static final long serialVersionUID = -6617807679829663071L;

  private transient String ip = null;
  private transient String port = null;
  private transient String database = null;
  private transient String user = null;
  private transient String password = null;
  private String driverClass = "com.microsoft.jdbc.sqlserver.SQLServerDriver";
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
  public SqlServerExectue(String ip, String port, String database, String user, String password) {
    this.ip = ip;
    this.port = port;
    this.database = database;
    this.user = user;
    this.password = password;
  }

  @Override
  public Connection getConnection() {
    try {
      if (null == con || con.isClosed()) {
        int time = 0;
        String url = "jdbc:sqlserver://" + this.ip + ":" + this.port + ";DatabaseName=" + this.database;
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
      System.out.println("数据库链接获取失败.");
      e.printStackTrace();
    }
    return con;
  }

  @Override
  public String getDriverClass() {
    return this.driverClass;
  }

}
