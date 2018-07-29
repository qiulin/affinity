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

package io.amient.affinity.core.config;

import com.typesafe.config.Config;
import com.typesafe.config.ConfigFactory;

import java.net.URL;
import java.nio.file.Path;
import java.util.*;
import java.util.concurrent.atomic.AtomicReference;

public class CfgStruct<T extends CfgStruct> extends Cfg<T> implements CfgNested {

    public final List<Options> options;

    protected List<Map.Entry<String, Cfg<?>>> properties = new LinkedList<>();

    protected List<Map.Entry<String, CfgString>> externalProperties = new LinkedList<>();

    private Config config;

    protected CfgStruct<?> parent = null;
    private List<CfgStruct<?>> children = new LinkedList<>();

    Set<String> extensions = new HashSet<String>() {{
        addAll(specializations());
    }};

    protected Set<String> specializations() {
        return Collections.emptySet();
    }

    protected void addChild(CfgStruct<?> child) {
        children.add(child);
    }

    @Override
    public CfgStruct<T> doc(String description) {
        super.doc(description);
        return this;
    }

    @Override
    public boolean isDefined() {
        return properties.stream().filter(p -> p.getValue().isRequired() && !p.getValue().isDefined()).count() == 0;
    }

    public CfgStruct(Class<? extends CfgStruct<?>> inheritFrom, Options... options) {
        this.options = Arrays.asList(options);
        try {
            CfgStruct<?> inheritedCfg = inheritFrom.newInstance();
            inheritedCfg.properties.forEach(p -> extensions.add(p.getKey()));
            inheritedCfg.extensions.forEach(e -> extensions.add(e));
        } catch (Exception e) {
            throw new RuntimeException(e);
        }
        setValue((T) this);
    }

    public CfgStruct() {
        this(Options.STRICT);
    }

    public CfgStruct(Options... options) {
        this.options = Arrays.asList(options);
        setValue((T) this);
    }

    @Override
    public void setPath(String path) {
        super.setPath(path);
        properties.forEach(entry -> entry.getValue().setPath(path(entry.getKey())));
    }

    public final T apply(CfgStruct<?> conf) throws IllegalArgumentException {
        if (conf.path() == null) throw new IllegalArgumentException();
        if (conf.parent != null && conf.parent.getClass().isAssignableFrom(this.getClass())) {
            return (T) conf.parent;
        } else {
            T z = apply(conf.config());
            final AtomicReference<T> self = new AtomicReference<>(z);
            self.get().setPath(conf.path());
            conf.children.forEach(x -> {
                if (self.get().getClass().isAssignableFrom(x.getClass())) {
                    self.set((T) x);
                }
            });
            T result = self.get();
            result.parent = conf;
//            result.externalProperties = conf.externalProperties;
            if (result == z) conf.addChild(result);
            return result;
        }
    }

    public final T apply(Map<String, ?> config) {
        return apply(ConfigFactory.parseMap(config));
    }

    @Override
    public T apply(Config config) throws IllegalArgumentException {
        if (config != null) {
            this.config = path().isEmpty() ? config : listPos > -1
                    ? config.getConfigList(relPath).get(listPos) : config.getConfig(relPath);
            final StringBuilder errors = new StringBuilder();
            properties.forEach(entry -> {
                String propPath = entry.getKey();
                Cfg<?> cfg = entry.getValue();
                try {
                    if (propPath == null || propPath.isEmpty()) {
                        cfg.apply(this.config);
                    } else if (this.config.hasPath(propPath)) {
                        cfg.apply(this.config);
                    }
                    if (cfg.required && !cfg.isDefined()) {
                        throw new IllegalArgumentException(propPath + " is required" + (path().isEmpty() ? "" : " in " + path()));
                    }
                } catch (IllegalArgumentException e) {
                    errors.append(e.getMessage() + "\n");
                }
            });

            externalProperties.clear();
            //if (!options.contains(Options.IGNORE_UNKNOWN)) {
            this.config.entrySet().forEach(entry -> {
                boolean existingProperty = properties.stream().filter((p) ->
                        p.getKey().equals(entry.getKey())
                                || (p.getValue() instanceof CfgNested && entry.getKey().startsWith(p.getKey() + "."))
                ).count() > 0;
                boolean allowedViaExtensions = extensions.stream().filter((s) ->
                        s.equals(entry.getKey()) || entry.getKey().startsWith(s + ".")
                ).count() > 0;
                if (!existingProperty && !allowedViaExtensions) {
                    if (!options.contains(Options.IGNORE_UNKNOWN)) {
                        errors.append(entry.getKey() + " is not a known property" + (path().isEmpty() ? "" : " of " + path()) + "\n");
                    } else {
                        CfgString c = new CfgString();
                        c.setValue(entry.getValue().unwrapped().toString());
                        externalProperties.add(new AbstractMap.SimpleEntry<String, CfgString>(entry.getKey(), c));
                    }
                }
            });
            String errorMessage = errors.toString();
            if (!errorMessage.isEmpty()) {
                throw new IllegalArgumentException(errorMessage);
            }
            return (T) this;
        }
        return (T) this;
    }

    @Override
    public String parameterInfo() {
        return "";
    }

    Config config() {
        return config;
    }

    public CfgString string(String path, boolean required) {
        return add(path, new CfgString(), required, Optional.empty());
    }

    public CfgString string(String path, String defaultValue) {
        return add(path, new CfgString(), true, Optional.of(defaultValue));
    }

    public CfgStringList stringlist(String path, List<String> defaultValue) {
        return add(path, new CfgStringList(), true, Optional.of(defaultValue));
    }

    public CfgStringList stringlist(String path, boolean required) {
        return add(path, new CfgStringList(), required, Optional.empty());
    }

    public CfgLong longint(String path, boolean required) {
        return add(path, new CfgLong(), required, Optional.empty());
    }

    public CfgLong longint(String path, long defaultValue) {
        return add(path, new CfgLong(), true, Optional.of(defaultValue));
    }

    public CfgBool bool(String path, boolean required) {
        return add(path, new CfgBool(), required, Optional.empty());
    }

    public CfgBool bool(String path, boolean required, boolean defaultValue) {
        return add(path, new CfgBool(), required, Optional.of(defaultValue));
    }


    public CfgInt integer(String path, boolean required) {
        return add(path, new CfgInt(), required, Optional.empty());
    }

    public CfgInt integer(String path, Integer defaultValue) {
        return add(path, new CfgInt(), true, Optional.of(defaultValue));
    }

    public CfgIntList intlist(String path, boolean required) {
        return add(path, new CfgIntList(), required, Optional.empty());
    }

    public CfgIntList intlist(String path, List<Integer> defaultValue) {
        return add(path, new CfgIntList(), true, Optional.of(defaultValue));
    }

    public CfgUrl url(String path, boolean required) {
        return add(path, new CfgUrl(), required, Optional.empty());
    }

    public CfgUrl url(String path, URL defaultVal) {
        return add(path, new CfgUrl(), true, Optional.of(defaultVal));
    }

    public CfgPath filepath(String path, boolean required) {
        return add(path, new CfgPath(), required, Optional.empty());
    }

    public CfgPath filepath(String path, Path defaultVal) {
        return add(path, new CfgPath(), true, Optional.of(defaultVal));
    }

    public <X> CfgCls<X> cls(String path, Class<X> c, boolean required) {
        return add(path, new CfgCls<>(c), required, Optional.empty());
    }

    public <X> CfgCls<X> cls(String path, Class<X> c, Class<? extends X> defaultVal) {
        return add(path, new CfgCls<>(c), true, Optional.of(defaultVal));
    }

    public <X extends CfgStruct<X>> X struct(String path, X obj, boolean required) {
        X x = add(path, obj, required, Optional.empty());
        obj.setValue(obj);
        return x;
    }

    public <X extends CfgStruct<X>> X ref(X obj, boolean required) {
        return add(null, obj, required, Optional.empty());
    }


    public <X extends Cfg<?>> CfgGroup<X> group(String path, Class<X> c, boolean required) {
        return add(path, new CfgGroup<>(c), required, Optional.empty());
    }

    public <X extends Cfg<?>> CfgGroup<X> group(String path, CfgGroup<X> obj, boolean required) {
        return add(path, obj, required, Optional.empty());
    }

    public <X> CfgList<X> list(String path, Class<X> c, boolean required) {
        return add(path, new CfgList(c), required, Optional.empty());
    }


    private <Y, X extends Cfg<Y>> X add(String itemRelPath, X cfg, boolean required, Optional<Y> defaultValue) {
        cfg.setRelPath(itemRelPath);
        cfg.setPath(path() + itemRelPath);
        if (defaultValue.isPresent()) cfg.setDefaultValue(defaultValue.get());
        if (!required) cfg.setOptional();
        properties.add(new AbstractMap.SimpleEntry<>(itemRelPath, cfg));
        return cfg;

    }

    @Override
    public boolean equals(Object other) {
        if (!(other instanceof CfgStruct)) {
            return false;
        } else {
            CfgStruct<T> that = (CfgStruct<T>) other;
            if (this.properties.size() != that.properties.size()) return false;
            for (int i = 0; i < this.properties.size(); i++) {
                Map.Entry<String, Cfg<?>> left = this.properties.get(i);
                Map.Entry<String, Cfg<?>> right = that.properties.get(i);
                boolean same = left.getKey().equals(right.getKey()) && left.getValue().equals(right.getValue());
                if (!same) return false;
            }
            return true;
        }
    }

    @Override
    public String toString() {
        final StringBuilder result = new StringBuilder();
        result.append("{");
        properties.forEach(entry -> {
            if (result.length() > 1) result.append(", ");
            result.append(entry.getKey());
            result.append(": ");
            result.append(entry.getValue());
        });
        result.append("}");
        return result.toString();
    }

    public Map<String, Cfg<?>> toMap() {
        TreeMap<String, Cfg<?>> result = new TreeMap<>();
        properties.forEach(prop -> result.put(prop.getKey(), prop.getValue()));
        externalProperties.forEach(prop -> result.put(prop.getKey(), prop.getValue()));
        return result;
    }

}
