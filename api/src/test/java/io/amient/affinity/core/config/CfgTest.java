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
import com.typesafe.config.ConfigRenderOptions;
import com.typesafe.config.ConfigValueFactory;
import io.amient.affinity.core.util.TimeCryptoProof;
import io.amient.affinity.core.util.TimeCryptoProofSHA256;
import org.junit.Rule;
import org.junit.Test;
import org.junit.rules.ExpectedException;

import javax.xml.soap.Node;
import java.util.*;

import static org.junit.Assert.assertEquals;
import static org.junit.Assert.assertFalse;
import static org.junit.Assert.assertTrue;

public class CfgTest {

    public static NodeConfig NodeConfig = new NodeConfig() {
        @Override
        public NodeConfig apply(Config config) throws IllegalArgumentException {
            return new NodeConfig().apply(config);
        }
    };

    public static class NodeConfig extends CfgStruct<NodeConfig> {
        private CfgLong StartupTimeoutMs = longint("startup.timeout.ms", true);
        private CfgLong ShutdownTimeoutMs = longint("shutdown.timeout.ms", 60000L);
        private CfgGroup<ServiceConfig> Services = group("service", ServiceConfig.class, false);
        private CfgList<ExternalConfig> Externals = list("externals", ExternalConfig.class, false);
    }

    public static class ExternalConfig extends CfgStruct<ExternalConfig> {
        private CfgString Description = string("description", true);
    }

    public static class ServiceConfig extends CfgStruct<ServiceConfig> {
        private CfgCls<TimeCryptoProof> Class = cls("class", TimeCryptoProof.class, true);
        private CfgList IntList = intlist("intlist",false);
        private CfgGroup IntLists = group("lists", CfgIntList.class, false);
        private Cfg Undefined = string("undefined", false);
        private CfgGroup UndefinedGroup = group("undefined-group", UndefinedGroupConfig.class, false);
    }

    public static class ExtendedServiceConfig extends CfgStruct<ExtendedServiceConfig> {
        private CfgString Info = string("info", false);
        public ExtendedServiceConfig() {
            super(ServiceConfig.class, Options.STRICT);
        }
    }

    public static class UndefinedGroupConfig extends CfgStruct<UndefinedGroupConfig> {
        public UndefinedGroupConfig() {
            super(Options.IGNORE_UNKNOWN);
        }
    }

    @Rule
    public ExpectedException ex = ExpectedException.none();

    @Test
    public void reportMissingPropertiesOnlyIfRequired() {
        ex.expect(IllegalArgumentException.class);
        ex.expectMessage("startup.timeout.ms is required\n");
        NodeConfig.apply(ConfigFactory.empty());
    }

    @Test
    public void reportInvalidPropertyForDifferentStruct() {
        ex.expect(IllegalArgumentException.class);
        ex.expectMessage("class is required in service.wrongstruct\n" +
                "something.we.dont.recognize is not a known property of service.wrongstruct\n");
        Config config = ConfigFactory.parseMap(new HashMap<String, Object>() {{
            put(NodeConfig.StartupTimeoutMs.path(), 100L);
            put(NodeConfig.ShutdownTimeoutMs.path(), 1000L);
            put(NodeConfig.Services.apply("wrongstruct").path("something.we.dont.recognize"), 20);
        }});
        NodeConfig.apply(config);
    }

    @Test
    public void reportRecoginzieCorrectGroupType() {
        ex.expect(IllegalArgumentException.class);
        ex.expectMessage("com.typesafe.config.Config is not an instance of class io.amient.affinity.core.util.TimeCryptoProof\n");
        Config config = ConfigFactory.parseMap(new HashMap<String, Object>() {{
            put(NodeConfig.StartupTimeoutMs.path(), 100L);
            put(NodeConfig.ShutdownTimeoutMs.path(), 1000L);
            put(NodeConfig.Services.apply("wrongclass").Class.path(), Config.class.getName());
        }});
        NodeConfig.apply(config);
    }

    @Test
    public void recognizeCorrectlyConfigured() {
        Config config = ConfigFactory.parseMap(new HashMap<String, Object>() {{
            put(NodeConfig.StartupTimeoutMs.path(), 100L);
            put(NodeConfig.ShutdownTimeoutMs.path(), 1000L);
            put(NodeConfig.Services.apply("myservice").Class.path(), TimeCryptoProofSHA256.class.getName());
            put(NodeConfig.Services.apply("myservice").IntList.path(), Arrays.asList(1, 2, 3));
            put(NodeConfig.Services.apply("myservice").IntLists.apply("group1").path(), Arrays.asList(1, 2, 3));
            put(NodeConfig.Services.apply("myservice").IntLists.apply("group2").path(), Arrays.asList(4));
            put(NodeConfig.Services.apply("myservice").UndefinedGroup.path("some.group.member.attribute"), "x");
        }});

        Config configWithExternals = config.withValue(NodeConfig.Externals.path(), ConfigValueFactory.fromIterable(new ArrayList<Map<String, String>>() {{
            add(new HashMap<String, String>() {{ put("description", "abc"); }});
            add(new HashMap<String, String>() {{ put("description", "xyz"); }});
        }}));

        NodeConfig applied = NodeConfig.apply(configWithExternals);
        assertEquals(TimeCryptoProofSHA256.class, applied.Services.apply("myservice").Class.apply());
        assertEquals(Arrays.asList(1, 2, 3), applied.Services.apply("myservice").IntList.apply());
        assertEquals(Arrays.asList(1, 2, 3), applied.Services.apply("myservice").IntLists.apply("group1").apply());
        assertEquals(Arrays.asList(4), applied.Services.apply("myservice").IntLists.apply("group2").apply());
        assertTrue(applied.Services.isDefined());
        assertTrue(applied.Services.apply("myservice").Class.isDefined());
        assertTrue(applied.Services.apply("myservice").IntList.isDefined());
        assertFalse(applied.Services.apply("myservice").Undefined.isDefined());
        assertEquals("abc", applied.Externals.apply(0).Description.apply());
        assertEquals("xyz", applied.Externals.apply(1).Description.apply());
    }


    @Test
    public void cfgStructEqualityIsBasedOnEqualityOfProperties() {
        Config config = ConfigFactory.parseMap(new HashMap<String, Object>() {{
            put(NodeConfig.StartupTimeoutMs.path(), 100L);
            put(NodeConfig.Services.apply("service1").Class.path(), TimeCryptoProofSHA256.class.getName());
        }});
        NodeConfig conf1 = NodeConfig.apply(config);
        NodeConfig conf2 = NodeConfig.apply(config);
        NodeConfig conf3 = new NodeConfig();
        conf3.StartupTimeoutMs.setValue(100L);
        conf3.Services.setValue(new HashMap<String, ServiceConfig>(){{
            ServiceConfig serviceConf = new ServiceConfig();
            serviceConf.Class.setValue(TimeCryptoProofSHA256.class);
            put("service1", serviceConf);
        }});

        NodeConfig diffConf = NodeConfig.apply(ConfigFactory.parseMap(new HashMap<String, Object>() {{
            put(NodeConfig.StartupTimeoutMs.path(), 666L);
            put(NodeConfig.Services.apply("service1").Class.path(), TimeCryptoProofSHA256.class.getName());
        }}));
        assert(conf1 != conf2);
        assert(conf1.equals(conf2));
        assert(conf2 != conf3);
        assert(conf2.equals(conf3));
        assert(conf3 != diffConf);
        assert(!conf3.equals(diffConf));

        HashMap<NodeConfig, Integer> map = new HashMap<CfgTest.NodeConfig, Integer>() {{
            put(conf1, 1);
            put(conf2, 2);
            put(conf3, 3);
            put(diffConf, 4);
        }};

        assert(map.size() == 2);
        assert(map.get(conf3) == 3);
        assert(map.get(diffConf) == 4);


    }

    @Test
    public void specializeConfig() {
        ServiceConfig serviceConf = new ServiceConfig();
        assert(serviceConf.isDefined()); //structs can never be undefined
        assert(serviceConf.Class.path().equals("class"));
        NodeConfig nodeConfig = new NodeConfig();
        //attaching to en empty node
        serviceConf.apply(nodeConfig);
        assert(serviceConf.Class.path().equals("class"));
        //attaching to a non-empty path
        assert(nodeConfig.isDefined()); //structs can never be undefined
        assert(nodeConfig.Services.path().equals("service"));
        serviceConf.apply(nodeConfig.Services.apply("x"));
        assert(serviceConf.Class.path().equals("service.x.class"));
    }

    @Test
    public void extendedConfig() {
        ServiceConfig serviceConf = new ServiceConfig();
        ExtendedServiceConfig extendedConfig = new ExtendedServiceConfig().apply(serviceConf);
        extendedConfig.Info.setValue("hello");
        assert(new ExtendedServiceConfig().apply(serviceConf).Info.apply().equals("hello"));
        assert(new ExtendedServiceConfig().apply(new ServiceConfig().apply(extendedConfig)).Info.apply().equals("hello"));

    }

}
