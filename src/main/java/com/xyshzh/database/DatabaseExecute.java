package com.xyshzh.database;

import java.lang.reflect.Method;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.ArrayList;
import java.util.HashMap;
import java.util.List;
import java.util.Map;

/**
 * 数据库操作通用接口.
 * 
 * @author Shengjun Liu
 * @date 2018-08-15
 */
@SuppressWarnings("serial")
public abstract class DatabaseExecute implements IDatabaseExecute {

  private List<PreparedStatement> psList = new ArrayList<>();

  public List<Map<String, Object>> getResultListMap(String sql, Object... objects) {
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = getPreparedStatement(sql, objects);
      rs = ps.executeQuery();
      rs.setFetchSize(FETCH_SIZE);
      ResultSetMetaData md = rs.getMetaData();
      while (rs.next()) {
        Map<String, Object> map = new HashMap<String, Object>();
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
      closeResource(rs, ps);
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  public <T> List<T> getResultList(String sql, String lable, Object... objects) {
    List<T> list = new ArrayList<>();
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = getPreparedStatement(sql, objects);
      rs = ps.executeQuery();
      rs.setFetchSize(FETCH_SIZE);
      while (rs.next()) {
        list.add((T) rs.getObject(lable));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      closeResource(rs, ps);
    }
    return list;
  }

  public Map<String, Object> getResultMap(String sql, Object... objects) {
    Map<String, Object> map = new HashMap<String, Object>();
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = getPreparedStatement(sql, objects);
      rs = ps.executeQuery();
      rs.setFetchSize(FETCH_SIZE);
      ResultSetMetaData md = rs.getMetaData();
      if (rs.next()) {
        for (int i = 1; i <= md.getColumnCount(); i++) {
          try {
            map.put(md.getColumnLabel(i), rs.getString(md.getColumnLabel(i)));
          } catch (Exception e) {
            map.put(md.getColumnName(i), rs.getString(md.getColumnName(i)));
          }
        }
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      closeResource(rs, ps);
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  public <T> T getResult(String sql, String lable, Object... objects) {
    PreparedStatement ps = null;
    ResultSet rs = null;
    T value = null;
    try {
      ps = getPreparedStatement(sql, objects);
      rs = ps.executeQuery();
      rs.setFetchSize(FETCH_SIZE);
      if (rs.next()) {
        value = (T) rs.getObject(lable);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      closeResource(rs, ps);
    }
    return value;
  }

  public PreparedStatement getPreparedStatement(String sql, Object... objects) throws SQLException {
    PreparedStatement ps = null;
    ps = getConnection().prepareStatement(sql);
    if (null != objects && objects.length > 0) {
      for (int i = 0; i < objects.length; i++) {
        ps.setObject((i + 1), objects[i]);
      }
    }
    return ps;
  }

  public ResultSet getResult(String sql, Integer fetchSize, Object... objects) {
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = getPreparedStatement(sql, objects);
      rs = ps.executeQuery();
      rs.setFetchSize(fetchSize);
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      // closeResource(ps);
      psList.add(ps);
    }
    return rs;
  }

  public void closeResource(Object... objects) {
    if (null != objects && 0 < objects.length) {
      for (Object obj : objects) {
        Method method = null;
        try {
          method = obj.getClass().getMethod("close");
          method.invoke(obj);
        } catch (RuntimeException | ReflectiveOperationException e) {
          e.printStackTrace();
        }
      }
    }
  }

  public void close() {
    // 关闭getResult时未关闭的PreparedStatement集合.
    closeResource(psList.toArray());
    // 关闭Connection数据库链接.
    closeResource(getConnection());
    System.out.println("数据库链接已执行关闭.");
  }

}
