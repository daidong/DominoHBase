/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements. See the NOTICE file distributed with this
 * work for additional information regarding copyright ownership. The ASF
 * licenses this file to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 * http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS, WITHOUT
 * WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied. See the
 * License for the specific language governing permissions and limitations
 * under the License.
 */

package org.apache.hadoop.hbase.trigger;

import java.util.Arrays;
import java.util.HashMap;

/**
 * Created with IntelliJ IDEA.
 * User: daidong
 * Date: 13-3-2
 * Time: 下午6:49
 * To change this template use File | Settings | File Templates.
 */
public class HTriggerKey {
    @Override
    public String toString() {
      return "HTriggerKey [tableName=" + new String(tableName)
          + ", columnFamily=" + new String(columnFamily) + ", column="
          + new String(column) + "]\n";
    }

    public byte[] tableName;
    public byte[] columnFamily;
    public byte[] column;

    public HTriggerKey(byte[] tableName, byte[] columnFamily, byte[] column){
        this.tableName = tableName;
        this.columnFamily = columnFamily;
        this.column = column;
    }

    public byte[] getTableName(){
      return this.tableName;
    }
    public byte[] getColumnFamily(){
      return this.columnFamily;
    }
    public byte[] getColumn(){
      return this.column;
    }
    @Override
    public boolean equals(Object o) {
        if (this == o) return true;
        if (o == null || getClass() != o.getClass()) return false;

        HTriggerKey that = (HTriggerKey) o;
        
        if (!Arrays.equals(columnFamily, that.columnFamily)) return false;
        if (!Arrays.equals(tableName, that.tableName)) return false;

        /**
         * There is possible that we do not set column when we submit the trigger.
         * In this case, we should not compare column any more.
         * What we do here is: 
         * 1) judge whether this.column or that.column equals to "*". 
         * 2) if yes, return true;
         * 2) if not, compare their byte
         */
        if ("*".compareToIgnoreCase(new String(column)) == 0)
          return true;
        if ("*".compareToIgnoreCase(new String(that.column)) == 0)
          return true;
        
        if (!Arrays.equals(column, that.column)) return false;
        return true;
    }

    @Override
    public int hashCode() {
        int result = tableName != null ? Arrays.hashCode(tableName) : 0;
        result = 31 * result + (columnFamily != null ? Arrays.hashCode(columnFamily) : 0);
        /**
         * We also should ignore column while computing hashCode of this object.
         */
        //result = 31 * result + (column != null ? Arrays.hashCode(column) : 0);
        return result;
    }
    
    public static void main(String[] args){
      String n1 = "hello";
      String cf1 = "content";
      String c1 = "zh";
      
      String n2 = "hello";
      String cf2 = "content";
      String c2 = "*";
      
      HTriggerKey ht1 = new HTriggerKey(n1.getBytes(), cf1.getBytes(), c1.getBytes());
      HTriggerKey ht2 = new HTriggerKey(n2.getBytes(), cf2.getBytes(), c2.getBytes());
      
      System.out.println(ht1.hashCode());
      System.out.println(ht2.hashCode());
      
      System.out.println(ht1==ht2);
      System.out.println(ht1.equals(ht2));
      
      HashMap<HTriggerKey, Integer> test = new HashMap<HTriggerKey, Integer>();
      test.put(ht1, 12);
      System.out.println(test.get(ht2));
    }
}
