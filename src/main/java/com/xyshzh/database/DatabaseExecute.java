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

  public List<Map<String, Object>> getResultListMap(String sql, Object... objects) {
    List<Map<String, Object>> list = new ArrayList<Map<String, Object>>();
    ResultSet rs = getResult(sql, 1000, objects);
    try {
      ResultSetMetaData md = rs.getMetaData();
      while (null != rs && rs.next()) {
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
      closeResource(rs);
    }
    return list;
  }

  @SuppressWarnings("unchecked")
  public <T> List<T> getResultList(String sql, String lable, Object... objects) {
    List<T> list = new ArrayList<>();
    ResultSet rs = getResult(sql, 1000, objects);
    try {
      while (null != rs && rs.next()) {
        list.add((T) rs.getObject(lable));
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      closeResource(rs);
    }
    return list;
  }

  public Map<String, Object> getResultMap(String sql, Object... objects) {
    Map<String, Object> map = new HashMap<String, Object>();
    ResultSet rs = getResult(sql, 1, objects);
    try {
      ResultSetMetaData md = rs.getMetaData();
      if (null != rs && rs.next()) {
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
      closeResource(rs);
    }
    return map;
  }

  @SuppressWarnings("unchecked")
  public <T> T getResult(String sql, String lable, Object... objects) {
    ResultSet rs = getResult(sql, 1, objects);
    T value = null;
    try {
      if (null != rs && rs.next()) {
        value = (T) rs.getObject(lable);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      closeResource(rs);
    }
    return value;
  }

  public ResultSet getResult(String sql, Integer fetchSize, Object... objects) {
    PreparedStatement ps = null;
    ResultSet rs = null;
    try {
      ps = getConnection().prepareStatement(sql);
      if (null != objects && objects.length > 0) {
        for (int i = 0; i < objects.length; i++) {
          ps.setObject((i + 1), objects[i]);
        }
      }
      rs = ps.executeQuery();
      rs.setFetchSize(fetchSize);
    } catch (SQLException e) {
      e.printStackTrace();
    } finally {
      // closeResource(ps);
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

}
