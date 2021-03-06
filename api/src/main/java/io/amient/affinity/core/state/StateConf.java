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

package io.amient.affinity.core.state;

import com.typesafe.config.Config;
import io.amient.affinity.core.config.Cfg;
import io.amient.affinity.core.config.CfgStruct;
import io.amient.affinity.core.storage.LogStorageConf;
import io.amient.affinity.core.storage.MemStore.MemStoreConf;

public class StateConf extends CfgStruct<StateConf> {

    public Cfg<Integer> TtlSeconds = integer("ttl.sec", -1)
            .doc("Per-record expiration which will based off event-time if the data class implements EventTime trait");

    public Cfg<Boolean> External = bool("external", true, false)
            .doc("If the state is attached to a data stream which is populated and partitioned by an external process - external state becomes readonly. Number of partitions will be also detected from the underlying storage log.");

    public Cfg<Integer> Partitions = integer("partitions", false).doc("Number of partitions (this setting cannot be applied to state stores defined within a Keyspace)");

    public Cfg<Long> MinTimestampUnixMs = longint("min.timestamp.ms", 0L)
            .doc("Any records with timestamp lower than this value will be immediately dropped");

    public LogStorageConf Storage = struct("storage", new LogStorageConf(), false);

    public MemStoreConf MemStore = struct("memstore", new MemStoreConf(), true);

    public Cfg<Long> LockTimeoutMs = longint("lock.timeout.ms", 10000L)
            .doc("How long a lock can be held by a single thread before throwing a TimeoutException");

    public Cfg<Long> WriteTimeoutMs = longint("write.timeout.ms", 10000L)
            .doc("How long can any of the write operation on a global store take before throwing a TimeoutException");

    @Override
    public StateConf apply(Config config) throws IllegalArgumentException {
        StateConf self = super.apply(config);
        if (External.apply() && Partitions.isDefined()) {
            throw new IllegalArgumentException("State cannot be both external and have defined number of partitions");
        }
        return self;
    }
}

