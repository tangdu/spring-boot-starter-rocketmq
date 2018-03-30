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

package org.apache.rocketmq.spring.starter;

import cn.luban.commons.config.LubanConfig;
import com.fasterxml.jackson.databind.ObjectMapper;
import com.google.common.base.Joiner;
import com.google.common.collect.HashBasedTable;
import com.google.common.collect.Lists;
import com.google.common.collect.Table;
import lombok.extern.slf4j.Slf4j;
import org.apache.rocketmq.client.consumer.DefaultMQPushConsumer;
import org.apache.rocketmq.client.impl.MQClientAPIImpl;
import org.apache.rocketmq.client.producer.DefaultMQProducer;
import org.apache.rocketmq.common.protocol.heartbeat.MessageModel;
import org.apache.rocketmq.spring.starter.annotation.GroupRocketMQMessageListener;
import org.apache.rocketmq.spring.starter.annotation.RocketMQMessageListener;
import org.apache.rocketmq.spring.starter.core.DefaultRocketMQListenerContainer;
import org.apache.rocketmq.spring.starter.core.RocketMQListener;
import org.apache.rocketmq.spring.starter.core.RocketMQSender;
import org.apache.rocketmq.spring.starter.enums.ConsumeMode;
import org.apache.rocketmq.spring.starter.enums.SelectorType;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.InitializingBean;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.annotation.Qualifier;
import org.springframework.beans.factory.support.BeanDefinitionBuilder;
import org.springframework.beans.factory.support.DefaultListableBeanFactory;
import org.springframework.boot.autoconfigure.condition.ConditionalOnBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnClass;
import org.springframework.boot.autoconfigure.condition.ConditionalOnMissingBean;
import org.springframework.boot.autoconfigure.condition.ConditionalOnProperty;
import org.springframework.boot.context.properties.EnableConfigurationProperties;
import org.springframework.context.ApplicationContext;
import org.springframework.context.ApplicationContextAware;
import org.springframework.context.ConfigurableApplicationContext;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.annotation.Order;
import org.springframework.core.env.StandardEnvironment;
import org.springframework.util.Assert;
import org.springframework.util.StringUtils;

import javax.annotation.Resource;
import java.util.List;
import java.util.Map;
import java.util.Objects;
import java.util.Set;
import java.util.concurrent.atomic.AtomicLong;

import static org.apache.rocketmq.spring.starter.core.DefaultRocketMQListenerContainerConstants.*;

@Configuration
@EnableConfigurationProperties(RocketMQProperties.class)
@ConditionalOnClass(MQClientAPIImpl.class)
@Order
@Slf4j
public class RocketMQAutoConfiguration {

    @Bean
    @ConditionalOnClass(DefaultMQProducer.class)
    @ConditionalOnMissingBean(DefaultMQProducer.class)
    @ConditionalOnProperty(prefix = "spring.rocketmq", value = {"name-server", "producer.group"})
    public DefaultMQProducer mqProducer(RocketMQProperties rocketMQProperties) {

        RocketMQProperties.Producer producerConfig = rocketMQProperties.getProducer();
        String groupName = producerConfig.getGroup();
        Assert.hasText(groupName, "[spring.rocketmq.producer.group] must not be null");

        DefaultMQProducer producer = new DefaultMQProducer(producerConfig.getGroup());
        producer.setNamesrvAddr(rocketMQProperties.getNameServer());
        producer.setSendMsgTimeout(producerConfig.getSendMsgTimeout());
        producer.setRetryTimesWhenSendFailed(producerConfig.getRetryTimesWhenSendFailed());
        producer.setRetryTimesWhenSendAsyncFailed(producerConfig.getRetryTimesWhenSendAsyncFailed());
        producer.setMaxMessageSize(producerConfig.getMaxMessageSize());
        producer.setCompressMsgBodyOverHowmuch(producerConfig.getCompressMsgBodyOverHowmuch());
        producer.setRetryAnotherBrokerWhenNotStoreOK(producerConfig.isRetryAnotherBrokerWhenNotStoreOk());

        return producer;
    }

    @Bean
    @ConditionalOnClass(ObjectMapper.class)
    @ConditionalOnMissingBean(name = "rocketMQMessageObjectMapper")
    public ObjectMapper rocketMQMessageObjectMapper() {
        return new ObjectMapper();
    }

//    @Bean(destroyMethod = "destroy")
//    @ConditionalOnBean(DefaultMQProducer.class)
//    @ConditionalOnMissingBean(name = "rocketMQTemplate")
//    public RocketMQTemplate rocketMQTemplate(DefaultMQProducer mqProducer,
//        @Autowired(required = false)
//        @Qualifier("rocketMQMessageObjectMapper")
//            ObjectMapper objectMapper) {
//        RocketMQTemplate rocketMQTemplate = new RocketMQTemplate();
//        rocketMQTemplate.setProducer(mqProducer);
//        if (Objects.nonNull(objectMapper)) {
//            rocketMQTemplate.setObjectMapper(objectMapper);
//        }
//
//        return rocketMQTemplate;
//    }

    @Bean(destroyMethod = "destroy")
    @ConditionalOnBean(DefaultMQProducer.class)
    @ConditionalOnMissingBean(name = "rocketMQSender")
    public RocketMQSender rocketMQTemplate(DefaultMQProducer mqProducer,
            @Autowired(required = false)
            @Qualifier("rocketMQMessageObjectMapper")
            ObjectMapper objectMapper) {

        RocketMQSender rocketMQTemplate = new RocketMQSender();
        rocketMQTemplate.setProducer(mqProducer);
        if (Objects.nonNull(objectMapper)) {
            rocketMQTemplate.setObjectMapper(objectMapper);
        }

        return rocketMQTemplate;
    }

    @Configuration
    @ConditionalOnClass(DefaultMQPushConsumer.class)
    @EnableConfigurationProperties(RocketMQProperties.class)
    @ConditionalOnProperty(prefix = "spring.rocketmq", value = "name-server")
    @Order
    public static class ListenerContainerConfiguration implements ApplicationContextAware, InitializingBean {
        private ConfigurableApplicationContext applicationContext;

        private AtomicLong counter = new AtomicLong(0);

        @Resource
        private StandardEnvironment environment;

        @Resource
        private RocketMQProperties rocketMQProperties;

        private ObjectMapper objectMapper;

        public ListenerContainerConfiguration() {
        }

        @SuppressWarnings("SpringJavaInjectionPointsAutowiringInspection")
        @Autowired(required = false)
        public ListenerContainerConfiguration(
            @Qualifier("rocketMQMessageObjectMapper") ObjectMapper objectMapper) {
            this.objectMapper = objectMapper;
        }

        @Override
        public void setApplicationContext(ApplicationContext applicationContext) throws BeansException {
            this.applicationContext = (ConfigurableApplicationContext) applicationContext;
        }

        @Override
        public void afterPropertiesSet() {
            Map<String, Object> beans = this.applicationContext.getBeansWithAnnotation(RocketMQMessageListener.class);

            if (Objects.nonNull(beans)) {
                beans.forEach(this::registerContainer);
            }
            //合并同Topic消息
            combineMessageListener();
        }

        private void registerContainer(String beanName, Object bean) {
            Class<?> clazz = AopUtils.getTargetClass(bean);

            if (!RocketMQListener.class.isAssignableFrom(bean.getClass())) {
                throw new IllegalStateException(clazz + " is not instance of " + RocketMQListener.class.getName());
            }
            RocketMQListener rocketMQListener = (RocketMQListener) bean;
            RocketMQMessageListener annotation = clazz.getAnnotation(RocketMQMessageListener.class);
            BeanDefinitionBuilder beanBuilder = BeanDefinitionBuilder.rootBeanDefinition(DefaultRocketMQListenerContainer.class);
            beanBuilder.addPropertyValue(PROP_NAMESERVER, rocketMQProperties.getNameServer());
            beanBuilder.addPropertyValue(PROP_TOPIC, environment.resolvePlaceholders(annotation.topic().name()));
            //由于consumerGroup不能重复
            String consumerGroup="";
            if(!StringUtils.hasLength(annotation.consumerGroup())){
                //1.如果cosumerGroup不设置,TAG包含所有，默认为系统名称+Topic名称+Tag名称
                consumerGroup= Joiner.on("_").join(LubanConfig.getApplicationName(),
                        annotation.topic().name(),annotation.messageType().getName()).toUpperCase();
            }else{
                //2.取自定义
                consumerGroup= environment.resolvePlaceholders(annotation.consumerGroup());
            }
            beanBuilder.addPropertyValue(PROP_CONSUMER_GROUP, consumerGroup);
            beanBuilder.addPropertyValue(PROP_CONSUME_MODE, annotation.consumeMode());
            beanBuilder.addPropertyValue(PROP_CONSUME_THREAD_MAX, annotation.consumeThreadMax());
            beanBuilder.addPropertyValue(PROP_MESSAGE_MODEL, annotation.messageModel());
            beanBuilder.addPropertyValue(PROP_SELECTOR_EXPRESS, environment.resolvePlaceholders(annotation.messageType().getName()));
            beanBuilder.addPropertyValue(PROP_SELECTOR_TYPE, annotation.selectorType());
            beanBuilder.addPropertyValue(PROP_ROCKETMQ_LISTENER, rocketMQListener);
            if (Objects.nonNull(objectMapper)) {
                beanBuilder.addPropertyValue(PROP_OBJECT_MAPPER, objectMapper);
            }
            beanBuilder.setDestroyMethodName(METHOD_DESTROY);

            String containerBeanName = String.format("%s_%s", DefaultRocketMQListenerContainer.class.getName(), counter.incrementAndGet());
            DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) applicationContext.getBeanFactory();
            beanFactory.registerBeanDefinition(containerBeanName, beanBuilder.getBeanDefinition());

            DefaultRocketMQListenerContainer container = beanFactory.getBean(containerBeanName, DefaultRocketMQListenerContainer.class);

            if (!container.isStarted()) {
                try {
                    container.start();
                } catch (Exception e) {
                    log.error("started container failed. {}", container, e);
                    throw new RuntimeException(e);
                }
            }

            log.info("register rocketMQ listener to container, listenerBeanName:{}, containerBeanName:{}", beanName, containerBeanName);
        }



        //将同相同topic的监听合并到
        private void combineMessageListener(){
            Map<String, Object> beans = this.applicationContext.getBeansWithAnnotation(GroupRocketMQMessageListener.class);
            if (Objects.isNull(beans)) {
                return;
            }
            Table<String,String,RocketMQListener> rocketMQMessageListenerHashBasedTables= HashBasedTable.create();
            for (Map.Entry<String, Object> entry : beans.entrySet()) {
                Object bean=entry.getValue();
                Class<?> clazz = AopUtils.getTargetClass(bean);
                if (!RocketMQListener.class.isAssignableFrom(bean.getClass())) {
                    throw new IllegalStateException(clazz + " is not instance of " + RocketMQListener.class.getName());
                }
                RocketMQListener rocketMQListener = (RocketMQListener) bean;
                GroupRocketMQMessageListener annotation = clazz.getAnnotation(GroupRocketMQMessageListener.class);
                rocketMQMessageListenerHashBasedTables.put(annotation.topic().name(),annotation.messageType().getName(),rocketMQListener);
            }
            //获取所有监听的Topic分类
            Set<String> topics=rocketMQMessageListenerHashBasedTables.rowKeySet();
            for (String topic : topics) {
                Map<String, RocketMQListener> rocketMQListenerMap = rocketMQMessageListenerHashBasedTables.row(topic);
                //tags合并
                String tags = Joiner.on("||").join(rocketMQListenerMap.keySet());
                registerGroupContainer(topic,tags,rocketMQListenerMap);
            }
        }

        private void registerGroupContainer(String topicName, String tags ,Map<String, RocketMQListener> rocketMQListenerMap) {
            RocketMQListener rocketMQListener = rocketMQListenerMap.values().iterator().next();
            GroupRocketMQMessageListener annotation = rocketMQListener.getClass().getAnnotation(GroupRocketMQMessageListener.class);
            BeanDefinitionBuilder beanBuilder = BeanDefinitionBuilder.rootBeanDefinition(DefaultRocketMQListenerContainer.class);
            beanBuilder.addPropertyValue(PROP_NAMESERVER, rocketMQProperties.getNameServer());
            beanBuilder.addPropertyValue(PROP_TOPIC, environment.resolvePlaceholders(annotation.topic().name()));
            beanBuilder.addPropertyValue(PROP_CONSUMER_GROUP, Joiner.on("_").join(LubanConfig.getApplicationName(),topicName).toUpperCase());
            //TODO 默认只支持集群消费+无顺序
            beanBuilder.addPropertyValue(PROP_CONSUME_MODE, ConsumeMode.CONCURRENTLY);
            beanBuilder.addPropertyValue(PROP_CONSUME_THREAD_MAX, 64);
            beanBuilder.addPropertyValue(PROP_MESSAGE_MODEL, MessageModel.CLUSTERING);
            beanBuilder.addPropertyValue(PROP_SELECTOR_EXPRESS, environment.resolvePlaceholders(tags));
            beanBuilder.addPropertyValue(PROP_SELECTOR_TYPE, SelectorType.TAG);
            beanBuilder.addPropertyValue(PROP_ROCKETMQ_LISTENER, rocketMQListenerMap);
            if (Objects.nonNull(objectMapper)) {
                beanBuilder.addPropertyValue(PROP_OBJECT_MAPPER, objectMapper);
            }
            beanBuilder.setDestroyMethodName(METHOD_DESTROY);

            String containerBeanName = String.format("group_%s_%s", DefaultRocketMQListenerContainer.class.getName(), counter.incrementAndGet());
            DefaultListableBeanFactory beanFactory = (DefaultListableBeanFactory) applicationContext.getBeanFactory();
            beanFactory.registerBeanDefinition(containerBeanName, beanBuilder.getBeanDefinition());

            DefaultRocketMQListenerContainer container = beanFactory.getBean(containerBeanName, DefaultRocketMQListenerContainer.class);

            if (!container.isStarted()) {
                try {
                    container.start();
                } catch (Exception e) {
                    log.error("started groupContainer failed. {}", container, e);
                    throw new RuntimeException(e);
                }
            }
            List<String> names= Lists.newArrayList();
            for (RocketMQListener listener : rocketMQListenerMap.values()) {
                names.add(listener.getClass().getName());
            }
            log.info("register rocketMQ listener to groupContainer, listenerBeanName:{}, containerBeanName:{}",Joiner.on(",").join(names) , containerBeanName);
        }
    }

}
