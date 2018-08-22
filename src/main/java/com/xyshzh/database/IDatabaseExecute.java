package com.xyshzh.database;

import java.io.Serializable;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.util.List;
import java.util.Map;

/**
 * 数据库操作通用接口.
 * 
 * @author Shengjun Liu
 * @date 2018-08-15
 */
public interface IDatabaseExecute extends Serializable {

  static int FETCH_SIZE = 1000;

  /**
   * 获取数据库链接.
   * 
   * @return Connection
   */
  Connection getConnection();

  /**
   * 获取查询结果, 适合返回结果可能不唯一, 字段可能不唯一.
   * 
   * @param sql
   *          执行语句.
   * @param objects
   *          注入参数列表.
   * @return Map
   */
  List<Map<String, Object>> getResultListMap(String sql, Object... objects);

  /**
   * 获取T类型查询结果集合, 适合返回结果可能不唯一, 但字段唯一.
   * 
   * @param sql
   *          执行语句.
   * @param lable
   *          获取结果字段名称.
   * @param objects
   *          注入参数列表.
   * @return List
   */
  <T> List<T> getResultList(String sql, String lable, Object... objects);

  /**
   * 获取查询结果, 适合返回结果唯一, 但字段可能不唯一.
   * 
   * @param sql
   *          执行语句.
   * @param objects
   *          注入参数列表.
   * @return Map
   */
  Map<String, Object> getResultMap(String sql, Object... objects);

  /**
   * 获取T类型查询结果, 适合返回结果唯一, 且字段唯一.
   * 
   * @param sql
   *          执行语句.
   * @param lable
   *          获取结果字段名称.
   * @param objects
   *          注入参数列表.
   * @return T
   */
  <T> T getResult(String sql, String lable, Object... objects);

  /**
   * 获取PreparedStatement.
   * 
   * @param sql
   *          执行语句.
   * @param objects
   *          注入参数列表.
   * @return PreparedStatement
   * @throws SQLException
   */
  PreparedStatement getPreparedStatement(String sql, Object... objects) throws SQLException;

  /**
   * 获取查询结果集合.
   * 
   * @param sql
   *          执行语句.
   * @param fetchSize
   *          批次大小.
   * @param objects
   *          注入参数列表.
   * @return ResultSet
   */
  ResultSet getResult(String sql, Integer fetchSize, Object... objects);

  /**
   * 获取数据库驱动.
   * 
   * @return String
   */
  String getDriverClass();

  /**
   * 关闭数据库资源.
   * 
   * @param objects
   *          as Connection or as Statement or as PreparedStatement or as
   *          ResultSet.
   */
  void closeResource(Object... objects);

  /**
   * 关闭数据库资源.
   */
  void close();
}
