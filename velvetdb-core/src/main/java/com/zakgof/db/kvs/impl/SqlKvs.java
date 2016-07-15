package com.zakgof.db.kvs.impl;

import java.io.ByteArrayInputStream;
import java.sql.Connection;
import java.sql.PreparedStatement;
import java.sql.ResultSet;
import java.sql.SQLException;
import java.sql.Statement;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.List;
import java.util.Map.Entry;

import com.zakgof.db.kvs.IKvs;
import com.zakgof.serialize.ISerializer;
import com.zakgof.serialize.ZeSerializer;
import com.zakgof.tools.Buffer;

public class SqlKvs implements IKvs, ILockable {

  private final Connection connection;
  private final ISerializer serializer;

  public SqlKvs(Connection connection) {
    this(connection, new ZeSerializer());    
  }
  
  public SqlKvs(Connection connection, ISerializer serializer) {
    this.connection = connection;
    this.serializer = serializer;
  }

  public static void checkDb(Connection connection) {
    try {
      Statement statement = connection.createStatement();
      statement.execute("CREATE TABLE IF NOT EXISTS nodes ( key1 TINYBLOB, value MEDIUMBLOB, INDEX keyidx(key1(64)))");      
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static void clean(Connection connection) {
    try {
      Statement statement = connection.createStatement();
      
      statement.execute("DROP TABLE IF EXISTS nodes");
      
      String dbname = connection.getMetaData().getDatabaseProductName().toLowerCase();
      if (dbname.equals("sqlite"))
    	  statement.execute("CREATE TABLE nodes ( key1 TINYBLOB PRIMARY KEY, value MEDIUMBLOB)");
      else
    	  statement.execute("CREATE TABLE nodes ( key1 TINYBLOB, value MEDIUMBLOB, INDEX keyidx(key1(64)))");
            
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
  
  public static void duplicate(Connection dst, Connection src) {
    clean(dst);    
    try (PreparedStatement insert = dst.prepareStatement("INSERT INTO nodes (key1, value) VALUES (?, ?)");
         PreparedStatement select = src.prepareStatement("SELECT key1, value FROM nodes")) {
        ResultSet rs = select.executeQuery();
        while (rs.next()) {
          byte[] keyBytes = rs.getBytes(1);
          byte[] valueBytes = rs.getBytes(2);   
          insert.setBytes(1, keyBytes);
          insert.setBytes(2, valueBytes);
          insert.execute();
          System.err.println(Arrays.toString(keyBytes));
        }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
  
  
  @Override
  public <T> T get(Class<T> clazz, Object key) {
    byte[] keyBytes = serializer.serialize(key);
    return getByKeyBytes(clazz, keyBytes);
  }

  @Override
  public <T> void put(Object key, T value) {
    byte[] valueBytes = serializer.serialize(value);
    byte[] keyBytes = serializer.serialize(key);
    if (!putByUpdate(valueBytes, keyBytes))
      putByInsert(valueBytes, keyBytes);
  }

  @Override
  public void delete(Object key) {
    byte[] keyBytes = serializer.serialize(key);
    try (PreparedStatement statement = connection.prepareStatement("DELETE FROM nodes WHERE key1=?")) {
      statement.setBytes(1, keyBytes);
      statement.execute();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private <T> T getByKeyBytes(Class<T> clazz, byte[] keyBytes) {
    try {
      try (PreparedStatement statement = connection.prepareStatement("SELECT value FROM nodes WHERE key1=?")) {        
        statement.setBytes(1, keyBytes);
        ResultSet rs = statement.executeQuery();
        List<T> list = extractValues(rs, clazz);
        if (list.size() > 1)
          throw new SQLException("Graph : multiple values with a key " + keyBytes);

        T result = list.isEmpty() ? null : list.get(0);
        return result;
      }
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }
  
  private <T> List<T> extractValues(ResultSet rs, Class<T> clazz) {
    try {
      List<T> values = new ArrayList<T>();
      while (rs.next()) {
        byte[] valueBytes = rs.getBytes(1);
        @SuppressWarnings("unchecked")
        T t = clazz == null ? (T) valueBytes : serializer.deserialize(new ByteArrayInputStream(valueBytes), clazz);
        values.add(t);
      }
      return values;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private boolean putByUpdate(byte[] valueBytes, byte[] keyBytes) {
    try (PreparedStatement statement = connection.prepareStatement("UPDATE nodes SET value=? WHERE key1=?")) {
      fillPutStmt(valueBytes, keyBytes, statement);
      int count = statement.executeUpdate();
      return count == 1;
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void putByInsert(byte[] valueBytes, byte[] keyBytes) {
    try (PreparedStatement statement = connection.prepareStatement("INSERT INTO nodes (value, key1) VALUES (?,?)")) {
      fillPutStmt(valueBytes, keyBytes, statement);
      statement.executeUpdate();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  private void fillPutStmt(byte[] valueBytes, byte[] keyBytes, PreparedStatement statement) {
    try {
      statement.setBytes(1, valueBytes);
      statement.setBytes(2, keyBytes);
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void lock(String lockName, long timeout) {
    try (PreparedStatement statement = connection.prepareStatement("SELECT GET_LOCK(?, ?)")) {
      statement.setString(1, lockName);
      statement.setLong(2, timeout);
      statement.execute();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  @Override
  public void unlock(String lockName) {
    try (PreparedStatement statement = connection.prepareStatement("SELECT RELEASE_LOCK(?)")) {
      statement.setString(1, lockName);
      statement.execute();
    } catch (SQLException e) {
      throw new RuntimeException(e);
    }
  }

  public void putRaw(Entry<Buffer, Buffer> entry) {
    byte[] valueBytes = entry.getValue().bytes();
    byte[] keyBytes = entry.getKey().bytes();    
    if (!putByUpdate(valueBytes, keyBytes))
      putByInsert(valueBytes, keyBytes);    
  }
   
}
