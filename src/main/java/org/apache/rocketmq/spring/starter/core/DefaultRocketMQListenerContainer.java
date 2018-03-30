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

package org.apache.rocketmq.spring.starter.core;

import cn.luban.commons.message.BaseMessage;
import com.alibaba.fastjson.JSON;
import com.alibaba.fastjson.JSONObject;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.collect.Maps;
import lombok.Getter;
import lombok.Setter;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.consumer.MessageSelector;
import org.apache.rocketmq.client.consumer.listener.*;
import org.apache.rocketmq.client.exception.MQClientException;
import org.apache.rocketmq.common.message.MessageExt;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.spring.starter.enums.ConsumeMode;
import org.apache.rocketmq.spring.starter.enums.SelectorType;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.util.Assert;

import java.lang.reflect.ParameterizedType;
import java.lang.reflect.Type;
import java.nio.charset.Charset;
import java.util.List;
import java.util.Map;
import java.util.Objects;

@SuppressWarnings("WeakerAccess")
@Slf4j
public class DefaultRocketMQListenerContainer implements InitializingBean, RocketMQListenerContainer {

    @Setter
    @Getter
    private long suspendCurrentQueueTimeMillis = 1000;

    /**
     * Message consume retry strategy<br> -1,no retry,put into DLQ directly<br> 0,broker control retry frequency<br>
     * >0,client control retry frequency
     */
    @Setter
    @Getter
    private int delayLevelWhenNextConsume = 0;

    @Setter
    @Getter
    private String consumerGroup;

    @Setter
    @Getter
    private String nameServer;

    @Setter
    @Getter
    private String topic;

    @Setter
    @Getter
    private ConsumeMode consumeMode = ConsumeMode.CONCURRENTLY;

    @Setter
    @Getter
    private SelectorType selectorType = SelectorType.TAG;

    @Setter
    @Getter
    private String selectorExpress = "*";

    @Setter
    @Getter
    private MessageModel messageModel = MessageModel.CLUSTERING;

    @Setter
    @Getter
    private int consumeThreadMax = 64;

    @Getter
    @Setter
    private String charset = "UTF-8";

    @Setter
    @Getter
    private ObjectMapper objectMapper = new ObjectMapper();

    @Setter
    @Getter
    private boolean started;

    @Setter
    private Map<String,RocketMQListener> rocketMQListeners;

    private DefaultMQPushConsumer consumer;

    private Map<String,Class> messageTypes;

    @Override
    public void setupMessageListeners(Map<String,RocketMQListener> rocketMQListeners) {
        this.rocketMQListeners = rocketMQListeners;
    }

    @Override
    public void destroy() {
        this.setStarted(false);
        if (Objects.nonNull(consumer)) {
            consumer.shutdown();
        }
        log.info("container destroyed, {}", this.toString());
    }

    public synchronized void start() throws MQClientException {

        if (this.isStarted()) {
            throw new IllegalStateException("container already started. " + this.toString());
        }

        initRocketMQPushConsumer();

        // parse message type
        this.messageTypes = getMessageType();
        log.debug("msgTypes: {}", messageTypes);

        consumer.start();
        this.setStarted(true);

        log.info("started container: {}", this.toString());
    }

    public class DefaultMessageListenerConcurrently implements MessageListenerConcurrently {

        @SuppressWarnings("unchecked")
        @Override
        public ConsumeConcurrentlyStatus consumeMessage(List<MessageExt> msgs, ConsumeConcurrentlyContext context) {
            for (MessageExt messageExt : msgs) {
                long now = System.currentTimeMillis();
                try {
                    rocketMQListeners.get(messageExt.getTags()).onMessage(doConvertMessage(messageExt));
                } catch (Exception e) {
                    log.warn("consume message failed. messageExt:{}", messageExt, e);
                    context.setDelayLevelWhenNextConsume(delayLevelWhenNextConsume);
                    return ConsumeConcurrentlyStatus.RECONSUME_LATER;
                } finally {
                    log.debug("consume:{},messageExt:{} cost:{} ms", messageExt.getMsgId(),messageExt,  System.currentTimeMillis() - now);
                }
            }

            return ConsumeConcurrentlyStatus.CONSUME_SUCCESS;
        }
    }

    public class DefaultMessageListenerOrderly implements MessageListenerOrderly {

        @SuppressWarnings("unchecked")
        @Override
        public ConsumeOrderlyStatus consumeMessage(List<MessageExt> msgs, ConsumeOrderlyContext context) {
            for (MessageExt messageExt : msgs) {
                long now = System.currentTimeMillis();
                try {
                    rocketMQListeners.get(messageExt.getTags()).onMessage(doConvertMessage(messageExt));
                } catch (Exception e) {
                    log.warn("consume message failed. message:{}", messageExt, e);
                    context.setSuspendCurrentQueueTimeMillis(suspendCurrentQueueTimeMillis);
                    return ConsumeOrderlyStatus.SUSPEND_CURRENT_QUEUE_A_MOMENT;
                } finally {
                    log.debug("consume:{},messageExt:{} cost:{} ms", messageExt.getMsgId(),messageExt,  System.currentTimeMillis() - now);
                }
            }

            return ConsumeOrderlyStatus.SUCCESS;
        }
    }

    @Override
    public void afterPropertiesSet() throws Exception {
        start();
    }

    @Override
    public String toString() {
        return "DefaultRocketMQListenerContainer{" +
            "consumerGroup='" + consumerGroup + '\'' +
            ", nameServer='" + nameServer + '\'' +
            ", topic='" + topic + '\'' +
            ", consumeMode=" + consumeMode +
            ", selectorType=" + selectorType +
            ", selectorExpress='" + selectorExpress + '\'' +
            ", messageModel=" + messageModel +
            '}';
    }

    @SuppressWarnings("unchecked")
    private Object doConvertMessage(MessageExt messageExt) {
        Class messageType = messageTypes.get(messageExt.getTags());
        if (Objects.equals(messageType, MessageExt.class)) {
            return messageExt;
        } else {
            String str = new String(messageExt.getBody(), Charset.forName(charset));
            if (Objects.equals(messageType, String.class)) {
                return str;
            } else {
                // if msgType not string, use objectMapper change it.
                try {
                    //对于实现BaseMessage接口数据特殊处理,默认取第一个实现接口
                    if(messageType.getInterfaces().length>0 && Objects.equals(messageType.getInterfaces()[0], BaseMessage.class)){
                        JSONObject jsonObject=JSONObject.parseObject(str);
                        jsonObject.put("messageId",messageExt.getMsgId());
                        return JSON.parseObject(jsonObject.toJSONString(),messageType);
                    }else {
                        return objectMapper.readValue(str, messageType);
                    }
                } catch (Exception e) {
                    log.info("convert failed. str:{}, msgType:{}", str, messageType);
                    throw new RuntimeException("cannot convert message to " + messageType, e);
                }
            }
        }
    }

    private Map<String,Class> getMessageType() {
        Map<String,Class> messageTypes= Maps.newHashMap();
        for (Map.Entry<String, RocketMQListener> entry : rocketMQListeners.entrySet()) {
            RocketMQListener rocketMQListener = entry.getValue();
            Type[] interfaces = rocketMQListener.getClass().getGenericInterfaces();
            if (Objects.nonNull(interfaces)) {
                Class<?> messageType=null;
                for (Type type : interfaces) {
                    if (type instanceof ParameterizedType) {
                        ParameterizedType parameterizedType = (ParameterizedType) type;
                        if (Objects.equals(parameterizedType.getRawType(), RocketMQListener.class)) {
                            Type[] actualTypeArguments = parameterizedType.getActualTypeArguments();
                            if (Objects.nonNull(actualTypeArguments) && actualTypeArguments.length > 0) {
                                messageType=(Class) actualTypeArguments[0];
                                break;
                            } else {
                                messageType=Object.class;
                                break;
                            }
                        }
                    }
                }
                if(messageType==null){
                    messageType=Object.class;
                }
                messageTypes.put(entry.getKey(),messageType);
            } else {
                messageTypes.put(entry.getKey(), Object.class);
            }
        }
        return messageTypes;
    }

    private void initRocketMQPushConsumer() throws MQClientException {

        Assert.notNull(rocketMQListeners, "Property 'rocketMQListener' is required");
        Assert.notNull(consumerGroup, "Property 'consumerGroup' is required");
        Assert.notNull(nameServer, "Property 'nameServer' is required");
        Assert.notNull(topic, "Property 'topic' is required");

        consumer = new DefaultMQPushConsumer(consumerGroup);
        consumer.setNamesrvAddr(nameServer);
        consumer.setConsumeThreadMax(consumeThreadMax);
        if (consumeThreadMax < consumer.getConsumeThreadMin()) {
            consumer.setConsumeThreadMin(consumeThreadMax);
        }

        consumer.setMessageModel(messageModel);

        switch (selectorType) {
            case TAG:
                consumer.subscribe(topic, selectorExpress);
                break;
            case SQL92:
                consumer.subscribe(topic, MessageSelector.bySql(selectorExpress));
                break;
            default:
                throw new IllegalArgumentException("Property 'selectorType' was wrong.");
        }

        switch (consumeMode) {
            case ORDERLY:
                consumer.setMessageListener(new DefaultMessageListenerOrderly());
                break;
            case CONCURRENTLY:
                consumer.setMessageListener(new DefaultMessageListenerConcurrently());
                break;
            default:
                throw new IllegalArgumentException("Property 'consumeMode' was wrong.");
        }

        // provide an entryway to custom setting RocketMQ consumer
        for (Map.Entry<String, RocketMQListener> entry : rocketMQListeners.entrySet()) {
            RocketMQListener rocketMQListener = entry.getValue();
            if (rocketMQListener instanceof RocketMQPushConsumerLifecycleListener) {
                ((RocketMQPushConsumerLifecycleListener) rocketMQListener).prepareStart(consumer);
            }
        }

    }

}
