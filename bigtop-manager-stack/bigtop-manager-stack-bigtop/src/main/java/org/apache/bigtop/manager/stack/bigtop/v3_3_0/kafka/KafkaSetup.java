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
package org.apache.bigtop.manager.stack.bigtop.v3_3_0.kafka;

import org.apache.bigtop.manager.common.shell.ShellResult;
import org.apache.bigtop.manager.stack.core.enums.ConfigType;
import org.apache.bigtop.manager.stack.core.spi.param.Params;
import org.apache.bigtop.manager.stack.core.utils.linux.LinuxFileUtils;

import lombok.AccessLevel;
import lombok.NoArgsConstructor;
import lombok.extern.slf4j.Slf4j;

import java.text.MessageFormat;
import java.util.Arrays;
import java.util.List;
import java.util.stream.Collectors;

import static org.apache.bigtop.manager.common.constants.Constants.PERMISSION_644;
import static org.apache.bigtop.manager.common.constants.Constants.PERMISSION_755;
import static org.apache.bigtop.manager.common.constants.Constants.ROOT_USER;

@Slf4j
@NoArgsConstructor(access = AccessLevel.PRIVATE)
public class KafkaSetup {

    private static void createKafkaDataDirs(String kafkaDataDir, String kafkaUser, String kafkaGroup) {
        // 兼容以下两种写法：
        // 1) 单目录：/data/kafka
        // 2) 多目录（Kafka 常见配置）：/data1/kafka,/data2/kafka
        // 同时容忍空格和重复逗号
        List<String> dirs = Arrays.stream(kafkaDataDir == null ? new String[0] : kafkaDataDir.split(","))
                .map(String::trim)
                .filter(s -> !s.isEmpty())
                .distinct()
                .collect(Collectors.toList());

        // 若未解析出任何目录，则保持原行为（可能由上层保证非空）
        if (dirs.isEmpty()) {
            LinuxFileUtils.createDirectories(kafkaDataDir, kafkaUser, kafkaGroup, PERMISSION_755, true);
            return;
        }

        for (String dir : dirs) {
            LinuxFileUtils.createDirectories(dir, kafkaUser, kafkaGroup, PERMISSION_755, true);
        }
    }

    public static ShellResult configure(Params params) {
        log.info("Configuring Kafka");
        KafkaParams kafkaParams = (KafkaParams) params;

        String confDir = kafkaParams.confDir();
        String kafkaUser = kafkaParams.user();
        String kafkaGroup = kafkaParams.group();

        // 支持多数据盘：当配置为逗号分隔的多个目录时，逐个创建
        createKafkaDataDirs(kafkaParams.getKafkaDataDir(), kafkaUser, kafkaGroup);
        LinuxFileUtils.createDirectories(kafkaParams.getKafkaLogDir(), kafkaUser, kafkaGroup, PERMISSION_755, true);
        LinuxFileUtils.createDirectories(kafkaParams.getKafkaPidDir(), kafkaUser, kafkaGroup, PERMISSION_755, true);

        LinuxFileUtils.toFile(
                ConfigType.PROPERTIES,
                MessageFormat.format("{0}/server.properties", confDir),
                kafkaUser,
                kafkaGroup,
                PERMISSION_644,
                kafkaParams.kafkaBroker(),
                kafkaParams.getGlobalParamsMap());

        LinuxFileUtils.toFileByTemplate(
                kafkaParams.getKafkaEnvContent(),
                MessageFormat.format("{0}/kafka-env.sh", confDir),
                kafkaUser,
                kafkaGroup,
                PERMISSION_644,
                kafkaParams.getGlobalParamsMap());

        LinuxFileUtils.toFileByTemplate(
                kafkaParams.getKafkaLog4jContent(),
                MessageFormat.format("{0}/log4j.properties", confDir),
                kafkaUser,
                kafkaGroup,
                PERMISSION_644,
                kafkaParams.getGlobalParamsMap());

        LinuxFileUtils.toFileByTemplate(
                kafkaParams.kafkaLimits(),
                MessageFormat.format("{0}/kafka.conf", KafkaParams.LIMITS_CONF_DIR),
                ROOT_USER,
                ROOT_USER,
                PERMISSION_644,
                kafkaParams.getGlobalParamsMap());

        log.info("Successfully configured Kafka");
        return ShellResult.success();
    }
}
