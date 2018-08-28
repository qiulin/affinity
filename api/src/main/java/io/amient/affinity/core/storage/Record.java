/*
 * Copyright 2016-2018 Michal Harish, michal.harish@gmail.com
 *
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *    http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package io.amient.affinity.core.storage;

import io.amient.affinity.core.util.EventTime;

import java.io.Serializable;

public class Record<K, V> implements Serializable {
    public final K key;
    public final V value;
    public final long timestamp;
    public final boolean tombstone;

    public Record(K key, V value) {
        this (key, value, value instanceof EventTime ? ((EventTime)value).eventTimeUnix() : EventTime.unix());
    }

    public Record(K key, V value, long timestamp) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.tombstone = false;
    }

    public Record(K key, V value, boolean tombstone) {
        this (key, value, value instanceof EventTime ? ((EventTime)value).eventTimeUnix() : EventTime.unix(), tombstone);
    }

    public Record(K key, V value, long timestamp, boolean tombstone) {
        this.key = key;
        this.value = value;
        this.timestamp = timestamp;
        this.tombstone = tombstone;
    }

    @Override
    public String toString() {
        return (key == null ? "null" : key.toString())
                + " " + (value == null ? "null" : value.toString())
                + " @ " + EventTime.local(timestamp);
    }

}