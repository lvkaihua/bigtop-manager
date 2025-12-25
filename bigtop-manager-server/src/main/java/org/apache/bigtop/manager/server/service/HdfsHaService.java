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
package org.apache.bigtop.manager.server.service;

import org.apache.bigtop.manager.server.model.req.EnableHdfsHaReq;

/**
 * HDFS HA service.
 *
 * 说明：当前仓库中 HdfsHaController 依赖该 Service，但历史版本中该文件缺失，导致编译失败。
 * 本接口用于修复该缺失问题，并为后续实现“单 NN -> 启用 HA”流程提供扩展点。
 */
public interface HdfsHaService {

    /**
     * Build a command to enable HDFS HA.
     *
     * 当前先提供最小实现所需的方法签名，用于通过编译。
     * 具体启用 HA 的编排逻辑（写入 hdfs-site/core-site、初始化 JN shared edits、bootstrap standby、formatZK、重启组件等）
     * 建议在实现类中完成。
     */
    org.apache.bigtop.manager.server.model.vo.CommandVO buildEnableHdfsHaCommand(Long clusterId, Long serviceId, EnableHdfsHaReq req);
}

