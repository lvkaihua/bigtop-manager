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

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;
import org.apache.bigtop.manager.common.constants.Constants;
import org.apache.bigtop.manager.common.shell.ShellResult;
import org.apache.bigtop.manager.spi.stack.Params;
import org.apache.bigtop.manager.stack.common.utils.LocalSettings;
import org.apache.bigtop.manager.stack.common.utils.linux.LinuxFileUtils;
import java.text.MessageFormat;
import java.util.HashMap;
import java.util.List;
import java.util.Map;
import static org.apache.bigtop.manager.common.constants.Constants.PERMISSION_755;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class SolrSetup {

    public static ShellResult config(Params params) {
        log.info("solrSetup config");
        SolrParams solrParams = (SolrParams) params;

        String confDir = solrParams.confDir();
        String solrUser = solrParams.user();
        String solrGroup = solrParams.group();
        Map<String, Object> solrEnv = solrParams.solrEnv();

        LinuxFileUtils.createDirectories(
                solrParams.getSolrDataDir(), solrUser, solrGroup, PERMISSION_755, true);
        LinuxFileUtils.createDirectories(
                solrParams.getSolrLogDir(), solrUser, solrGroup, PERMISSION_755, true);
        LinuxFileUtils.createDirectories(
                solrParams.getSolrPidDir(), solrUser, solrGroup, PERMISSION_755, true);


        // solr-env.xml
        List<String> zookeeperServerHosts = LocalSettings.hosts("zookeeper_server");
        Map<String, Object> paramMap = new HashMap<>();
        paramMap.put("zookeeper_quorum", zookeeperServerHosts);
        LinuxFileUtils.toFileByTemplate(
                solrEnv.get("content").toString(),
                MessageFormat.format("{0}/solr-env.xml", confDir),
                solrUser,
                solrGroup,
                Constants.PERMISSION_755,
                solrParams.getGlobalParamsMap(),
                paramMap);
        log.info("lvkaihua solr-env");
        // solr-log4j.xml
        LinuxFileUtils.toFileByTemplate(
                solrParams.solrLog4j().get("content").toString(),
                MessageFormat.format("{0}/solr-log4j.xml", confDir),
                solrUser,
                solrGroup,
                Constants.PERMISSION_755,
                solrParams.getGlobalParamsMap());
        log.info("lvkaihua solr-log4j");
        // solr-xml.xml
        LinuxFileUtils.toFileByTemplate(
                solrParams.solrxml().get("content").toString(),
                MessageFormat.format("{0}/solr.xml", confDir),
                solrUser,
                solrGroup,
                Constants.PERMISSION_755,
                solrParams.getGlobalParamsMap());
        log.info("lvkaihua solr-xml");

        return ShellResult.success("solr Server Configure success!");
    }
}
