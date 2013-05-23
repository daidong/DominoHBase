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

/**
 * 2013/05/20 ADD THE INITIAL ROUND SUPPORT
 * In Domino, developers should be able to set the trigger's type. These types include:
 * 1) INITIAL: current trigger need to run itself on the initial data once submitted
 * 2) CONVERGE: current trigger need to check the convergence of whole program. 
 * 3) ACCUMULATOR: current trigger is an accumulate trigger
 * 4) ORDINARY: default type
 */

public enum TRIGGERTYPE {
  ORDINARY ("ORDINARY"), //default trigger
  
  INITIAL ("INITIAL"),  //automatically run first round when submitted
  CONVERGE ("CONVERGE"), //need old value
  ACCUMULATOR ("ACCUMULATOR"),  //accmulate trigger
  
  INITIALWITHCONVERGE ("INITIALWITHCONVERGE"),
  ACCUMULATORWITHCONVERGE ("ACCUMULATORWITHCONVERGE");
  
  public String name;
  TRIGGERTYPE(String name){
    this.name = name;
  }
  
  public static TRIGGERTYPE fromString(String name){
    if (name != null){
      for (TRIGGERTYPE tt : TRIGGERTYPE.values()){
        if (name.equalsIgnoreCase(tt.toString()))
          return tt;
      }
    }
    return TRIGGERTYPE.ORDINARY;
  }
}
