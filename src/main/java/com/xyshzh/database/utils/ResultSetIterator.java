package com.xyshzh.database.utils;

import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.ResultSetMetaData;
import java.sql.SQLException;
import java.util.HashMap;
import java.util.Map;

import com.xyshzh.database.IDatabaseExecute;

/**
 * 数据结果迭代.
 * 
 * @author Shengjun Liu
 * @date 2018-08-27
 */
public class ResultSetIterator {

  private IDatabaseExecute execute = null;

  private PreparedStatement ps = null;
  private ResultSet rs = null;
  private ResultSetMetaData md = null;

  private long current = 0L;
  private boolean count_total = false;
  private long total = 0L;

  public ResultSetIterator(IDatabaseExecute execute) {
    this(execute, false);
  }

  public ResultSetIterator(IDatabaseExecute execute, boolean count_total) {
    this.execute = execute;
    this.count_total = count_total;
  }

  public ResultSetIterator execute(String sql, Object... objects) {
    try {
      ps = execute.getPreparedStatement(sql, objects);
      rs = ps.executeQuery();
      rs.setFetchSize(IDatabaseExecute.FETCH_SIZE);
      md = rs.getMetaData();
      if (count_total) {
        total = execute.getResult("select count(t.*) as c from (" + sql + ") as t", "c", objects);
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return this;
  }

  public synchronized Map<String, Object> getNextResult() {
    try {
      if (rs.next()) {
        current++;
        Map<String, Object> map = new HashMap<String, Object>();
        for (int i = 1; i <= md.getColumnCount(); i++) {
          try {
            map.put(md.getColumnLabel(i), rs.getString(md.getColumnLabel(i)));
          } catch (Exception e) {
            map.put(md.getColumnName(i), rs.getString(md.getColumnName(i)));
          }
        }
        return map;
      }
    } catch (SQLException e) {
      e.printStackTrace();
    }
    return null;
  }

  @Override
  public String toString() {
    return "当前位置:" + this.current + (0 < this.total ? "总记录数:" + this.total : "");
  }
}
