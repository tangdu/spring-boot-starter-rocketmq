/*
 * Licensed to the Apache Software Foundation (ASF) under one or more
 * contributor license agreements.  See the NOTICE file distributed with
 * this work for additional information regarding copyright ownership.
 * The ASF licenses this file to You under the Apache License, Version 2.0
 * (the "License"); you may not use this file except in compliance with
 * the License.  You may obtain a copy of the License at
 *
 *     http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */

package org.apache.rocketmq.spring.starter.annotation;

import cn.luban.commons.message.MessageType;
import cn.luban.commons.message.Topic;
import org.apache.rocketmq.common.filter.ExpressionType;

import java.lang.annotation.*;

/**
 * Topic组配置
 *
 * @author tangdu
 * @version $: GroupRocketMQ.java, v 0.1 2018年03月30日 上午11:24 tangdu Exp $
 */
@Target(ElementType.TYPE)
@Retention(RetentionPolicy.RUNTIME)
@Documented
public @interface GroupRocketMQMessageListener {

    /**
     * Topic name
     */
    Topic topic();

    /**
     * Control which message can be select. Grammar please see {@link ExpressionType#TAG} and {@link ExpressionType#SQL92}
     */
    MessageType messageType();

}
