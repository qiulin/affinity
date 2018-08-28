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

import io.amient.affinity.core.storage.Record;

import java.util.*;

public abstract class ObservableState<K> extends Observable {

    class ObservableKeyValue extends Observable {
        @Override
        public void notifyObservers(Object arg) {
            setChanged();
            super.notifyObservers(arg);
        }
    }

    /**
     * Observables are attached to individual keys
     */
    private Map<K, ObservableKeyValue> observables = new HashMap<>();

    private ObservableKeyValue getOrCreate(K key) {
        ObservableKeyValue observable = observables.get(key);
        if (observable == null) {
            observable = new ObservableKeyValue();
            observables.put(key, observable);
        }
        return observable;
    }

    public ObservableKeyValue addKeyValueObserver(K key, Observer observer) {
        ObservableKeyValue observable = getOrCreate(key);
        observable.addObserver(observer);
        return observable;
    }

    public Observer addKeyValueObserver(K key, Object initEvent, Observer observer) {
        ObservableKeyValue observable = getOrCreate(key);
        observable.addObserver(observer);
        observer.update(observable, initEvent);
        return observer;
    }

    public void removeKeyValueObserver(K key, Observer observer) {
        ObservableKeyValue observable = observables.get(key);
        if (observable != null) {
            observable.deleteObserver(observer);
            if (observable.countObservers() == 0) observables.remove(key);
        }
    }

    public void push(Record<K, ?> record) {
        try {
            ObservableKeyValue observable = observables.get(record.key);
            if (observable != null) {
                observable.notifyObservers(record);
            }
        } finally {
            setChanged();
            notifyObservers(record);
        }
    }

    public abstract void internalPush(Record<byte[], byte[]> value);

}
