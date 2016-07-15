package com.zakgof.db.velvet.test;

import java.io.Serializable;

public class KeylessEnt implements Serializable {
  
  private static final long serialVersionUID = -355147004979801748L;
  
  private int num;  
  private String str;
  
  public KeylessEnt() {
  }

  public KeylessEnt(int num, String str) {
    this.num = num;
    this.str = str;
  }
 
  @Override
  public String toString() {
    return num + " " + str;
  }
  
  public int getNum() {
    return num;
  }
  
  public String getStr() {
    return str;
  }

  @Override
  public int hashCode() {
    final int prime = 31;
    int result = 1;
    result = prime * result + num;
    result = prime * result + ((str == null) ? 0 : str.hashCode());
    return result;
  }

  @Override
  public boolean equals(Object obj) {
    if (this == obj)
      return true;
    if (obj == null)
      return false;
    if (getClass() != obj.getClass())
      return false;
    KeylessEnt other = (KeylessEnt) obj;
    if (num != other.num)
      return false;
    if (str == null) {
      if (other.str != null)
        return false;
    } else if (!str.equals(other.str))
      return false;
    return true;
  }
}