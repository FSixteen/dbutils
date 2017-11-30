package s.j.liu.dbutils;

import java.sql.Connection;
import java.sql.DriverManager;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * @version v0.0.1
 * @since 2017-07-22 11:55:41
 * @author Shengjun Liu
 *
 */
public class MysqlUtils {
  private transient String ip = null;
  private transient String port = null;
  private transient String database = null;
  private transient String user = null;
  private transient String password = null;
  private Connection con = null;

  /**
   * Construction method.
   * 
   * @param ip
   *          IP
   * @param port
   *          Port
   * @param database
   *          DB
   * @param user
   *          User
   * @param password
   *          Password
   */
  public MysqlUtils(String ip, String port, String database, String user, String password) {
    this.ip = ip;
    this.port = port;
    this.database = database;
    this.user = user;
    this.password = password;
  }

  /**
   * Get MySQL Connection Object.
   * 
   * @return Connection
   */
  public Connection getMysqlConnection() {
    int time = 0;
    String url = "jdbc:mysql://" + this.ip + ":" + this.port + "/" + this.database
        + "?useUnicode=true&characterEncoding=UTF-8"
        + "&autoReconnect=true&zeroDateTimeBehavior=convertToNull";
    try {
      for (; (++time < 3) && (null == this.con || this.con.isClosed());) {
        Class.forName("com.mysql.jdbc.Driver");
        con = DriverManager.getConnection(url, user, password);
      }
    } catch (Exception e) {
      System.out.println("Cannot get MySQL database connection.");
      e.printStackTrace();
    }
    return con;
  }

  /**
   * Static Method. Get MySQL Connection Object.
   * 
   * @param ip
   *          IP
   * @param port
   *          Port
   * @param database
   *          DB
   * @param user
   *          User
   * @param password
   *          Password
   */
  public static Connection getMysqlConnection(String ip, String port, String database, String user,
      String password) {
    int time = 0;
    String url = "jdbc:mysql://" + ip + ":" + port + "/" + database
        + "?useUnicode=true&characterEncoding=UTF-8"
        + "&autoReconnect=true&zeroDateTimeBehavior=convertToNull";
    Connection con = null;
    try {
      for (; (++time < 3) && (null == con || con.isClosed());) {
        Class.forName("com.mysql.jdbc.Driver");
        con = DriverManager.getConnection(url, user, password);
      }
    } catch (Exception e) {
      System.out.println("Cannot get MySQL database connection.");
      e.printStackTrace();
    }
    return con;
  }

  /**
   * Get Query Result.
   * 
   * @param con
   *          Connection Object
   * @param sql
   *          Execute Statement
   * @param lable
   *          Field Name
   * @param objects
   *          Genetic Parameter
   * @return List
   */
  public static List<String> getResultList(Connection con, String sql, String lable,
      Object... objects) {
    List<String> list = new ArrayList<String>();
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = con.prepareStatement(sql);
      if (null != objects && objects.length > 0) {
        for (int i = 0; i < objects.length; i++) {
          ps.setObject((i + 1), objects[i]);
        }
      }
      rs = ps.executeQuery();
      while (rs.next()) {
        list.add(rs.getString(lable));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      closeMysqlResource(rs, ps);
    }
    return list;
  }

  /**
   * Get Query Result.
   * 
   * @param con
   *          Connection Object
   * @param sql
   *          Execute Statement
   * @param key
   *          Field Name
   * @param value
   *          Field Name
   * @param objects
   *          Genetic Parameter
   * @return List
   */
  public static Map<String, String> getResultMap(Connection con, String sql, String key,
      String value, Object... objects) {
    Map<String, String> list = new HashMap<String, String>();
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = con.prepareStatement(sql);
      if (null != objects && objects.length > 0) {
        for (int i = 0; i < objects.length; i++) {
          ps.setObject((i + 1), objects[i]);
        }
      }
      rs = ps.executeQuery();
      while (rs.next()) {
        list.put(rs.getString(key), rs.getString(value));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      closeMysqlResource(rs, ps);
    }
    return list;
  }

  /**
   * Get Query Result.
   * 
   * @param con
   *          Connection Object
   * @param sql
   *          Execute Statement
   * @param objects
   *          Genetic Parameter
   * @return List
   */
  public static List<Map<String, String>> getResultListMap(Connection con, String sql,
      Object... objects) {
    List<Map<String, String>> list = new ArrayList<Map<String, String>>();
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = con.prepareStatement(sql);
      if (null != objects && objects.length > 0) {
        for (int i = 0; i < objects.length; i++) {
          ps.setObject((i + 1), objects[i]);
        }
      }
      rs = ps.executeQuery();
      ResultSetMetaData md = rs.getMetaData();
      while (rs.next()) {
        Map<String, String> map = new HashMap<String, String>();
        for (int i = 1; i <= md.getColumnCount(); i++) {
          try {
            map.put(md.getColumnLabel(i), rs.getString(md.getColumnLabel(i)));
          } catch (Exception e) {
            map.put(md.getColumnName(i), rs.getString(md.getColumnName(i)));
          }
        }
        list.add(map);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      closeMysqlResource(rs, ps);
    }
    return list;
  }

  /**
   * Get Query Result.
   * 
   * @param con
   *          Connection Object
   * @param sql
   *          Execute Statement
   * @param objects
   *          Genetic Parameter
   * @return long
   */
  public static long getResultLong(Connection con, String sql, Object... objects) {
    PreparedStatement ps = null;
    long value = 0;
    try {
      ps = con.prepareStatement(sql);
      if (null != objects && objects.length > 0) {
        for (int i = 0; i < objects.length; i++) {
          ps.setObject((i + 1), objects[i]);
        }
      }
      value = ps.executeUpdate();
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      closeMysqlResource(ps);
    }
    return value;
  }

  /**
   * Get Query Result.
   * 
   * @param con
   *          Connection Object
   * @param sql
   *          Execute Statement
   * @param lable
   *          Field Name
   * @param objects
   *          Genetic Parameter
   * @return long
   */
  public static String getResultString(Connection con, String sql, String lable,
      Object... objects) {
    PreparedStatement ps = null;
    ResultSet rs = null;
    String value = "";
    try {
      ps = con.prepareStatement(sql);
      if (null != objects && objects.length > 0) {
        for (int i = 0; i < objects.length; i++) {
          ps.setObject((i + 1), objects[i]);
        }
      }
      rs = ps.executeQuery();
      rs.next();
      value = rs.getString(lable);
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      closeMysqlResource(rs, ps);
    }
    return value;
  }

  /**
   * Close MySQL Resource.
   * 
   * @param objects
   *          as Connection or as Statement or as PreparedStatement or as
   *          ResultSet
   * @return boolean
   */
  public static boolean closeMysqlResource(Object... objects) {
    try {
      if (null != objects && objects.length > 0) {
        for (Object obj : objects) {
          if (obj instanceof Connection) {
            ((Connection) obj).close();
            continue;
          }
          if (obj instanceof Statement) {
            ((Statement) obj).close();
            continue;
          }
          if (obj instanceof ResultSet) {
            ((ResultSet) obj).close();
            continue;
          }
        }
      }
      return true;
    } catch (Exception e) {
      e.printStackTrace();
      return false;
    }
  }
}
