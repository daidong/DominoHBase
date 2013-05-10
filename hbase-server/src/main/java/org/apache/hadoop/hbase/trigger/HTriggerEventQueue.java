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

import java.util.Comparator;
import java.util.concurrent.ConcurrentLinkedQueue;
import java.util.concurrent.ConcurrentSkipListSet;

import org.apache.hadoop.hbase.util.EnvironmentEdgeManager;

/**
 * Created with IntelliJ IDEA. User: daidong Date: 13-3-2 Time: 下午9:32 To change
 * this template use File | Settings | File Templates.
 */
public class HTriggerEventQueue {

	private static ConcurrentLinkedQueue<HTriggerEvent> EventQueue = new ConcurrentLinkedQueue<HTriggerEvent>();

	private static Runnable consumer = null;

	public static void register(Runnable t) {
		HTriggerEventQueue.consumer = t;
	}

	/**
	 * @author daidong 2013/05/04 LOG: i modify append without check wether the
	 *         HTriggerEvent is inside EventQueue or not. The Reason is
	 *         Performnace is too low if we check each time wether current event
	 *         is or is not in the queue. It costs O(n) time!
	 * 
	 * @param hte
	 */
	public static void append(HTriggerEvent hte) {
		synchronized (consumer) {
			EventQueue.add(hte);
			consumer.notify();
		}
	}

	public static HTriggerEvent poll() throws InterruptedException {
		synchronized (consumer) {
			while (EventQueue.isEmpty()) {
				consumer.wait();
			}
			return EventQueue.poll();
		}
	}
}
