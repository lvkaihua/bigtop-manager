/*
 * Licensed to the Apache Software Foundation (ASF) under one
 * or more contributor license agreements.  See the NOTICE file
 * distributed with this work for additional information
 * regarding copyright ownership.  The ASF licenses this file
 * to you under the Apache License, Version 2.0 (the
 * "License"); you may not use this file except in compliance
 * with the License.  You may obtain a copy of the License at
 *
 *    https://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing,
 * software distributed under the License is distributed on an
 * "AS IS" BASIS, WITHOUT WARRANTIES OR CONDITIONS OF ANY
 * KIND, either express or implied.  See the License for the
 * specific language governing permissions and limitations
 * under the License.
 */
package org.apache.bigtop.manager.stack.bigtop.v3_3_0.solr;

import com.google.auto.service.AutoService;
import lombok.extern.slf4j.Slf4j;
import org.apache.bigtop.manager.common.shell.ShellResult;
import org.apache.bigtop.manager.spi.stack.Params;
import org.apache.bigtop.manager.spi.stack.Script;
import org.apache.bigtop.manager.stack.common.exception.StackException;
import org.apache.bigtop.manager.stack.common.utils.LocalSettings;
import org.apache.bigtop.manager.stack.common.utils.PackageUtils;
import org.apache.bigtop.manager.stack.common.utils.linux.LinuxOSUtils;

import java.io.IOException;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.Map;

@Slf4j
@AutoService(Script.class)
public class SolrServerScript implements Script {

    @Override
    public ShellResult install(Params params) {
        return PackageUtils.install(params.getPackageList());
    }

    @Override
    public ShellResult configure(Params params) {
        return SolrSetup.config(params);
    }

    @Override
    public ShellResult start(Params params) {
        configure(params);
        SolrParams solrParams = (SolrParams) params;
        Map env = new HashMap();
        String confdir = MessageFormat.format("{0}/solr-env.xml",solrParams.confDir());

        env.put("SOLR_INCLUDE",confdir);
        log.info(env.toString());
        log.info("lvkaihua1 "+ solrParams.solrEnv().toString());
//        String cmd = MessageFormat.format("{0}/bin/solr start -cloud -force", solrParams.serviceHome());
        String cmd = MessageFormat.format("{0}/bin/solr start -cloud -noprompt -s {1} -Dsolr.default.confdir={2} -z {3}", solrParams.serviceHome(),solrParams.getSolrDataDir(),solrParams.confDir(),solrParams.ZK_HOST());
        log.info(cmd + "lvkaihua");
        try {
//            return LinuxOSUtils.sudoExecCmd(cmd, solrParams.user());
            return LinuxOSUtils.sudoExecCmd(cmd, solrParams.user(),env);
        } catch (IOException e) {
            throw new StackException(e);
        }
    }

    @Override
    public ShellResult stop(Params params) {
        SolrParams solrParams = (SolrParams) params;
        String cmd = MessageFormat.format("{0}/bin/solr stop -all", solrParams.serviceHome());
        try {
            return LinuxOSUtils.sudoExecCmd(cmd, solrParams.user());
        } catch (IOException e) {
            throw new StackException(e);
        }
    }

    @Override
    public ShellResult status(Params params) {
        SolrParams solrParams = (SolrParams) params;
        return LinuxOSUtils.checkProcess(solrParams.getSolrPidFile());
    }
}
