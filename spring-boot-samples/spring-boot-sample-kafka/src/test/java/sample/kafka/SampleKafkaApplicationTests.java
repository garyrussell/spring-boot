/*
 * Copyright 2012-2018 the original author or authors.
 *
 * Licensed under the Apache License, Version 2.0 (the "License");
 * you may not use this file except in compliance with the License.
 * You may obtain a copy of the License at
 *
 *      http://www.apache.org/licenses/LICENSE-2.0
 *
 * Unless required by applicable law or agreed to in writing, software
 * distributed under the License is distributed on an "AS IS" BASIS,
 * WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
 * See the License for the specific language governing permissions and
 * limitations under the License.
 */
package sample.kafka;

import java.util.concurrent.CountDownLatch;
import java.util.concurrent.TimeUnit;

import org.aopalliance.intercept.MethodInterceptor;
import org.aopalliance.intercept.MethodInvocation;
import org.junit.Rule;
import org.junit.Test;
import org.junit.runner.RunWith;

import org.springframework.aop.framework.Advised;
import org.springframework.aop.framework.ProxyFactory;
import org.springframework.aop.support.AopUtils;
import org.springframework.beans.BeansException;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.beans.factory.config.BeanPostProcessor;
import org.springframework.boot.test.context.SpringBootTest;
import org.springframework.boot.test.rule.OutputCapture;
import org.springframework.context.annotation.Bean;
import org.springframework.context.annotation.Configuration;
import org.springframework.core.Ordered;
import org.springframework.kafka.test.context.EmbeddedKafka;
import org.springframework.test.context.TestPropertySource;
import org.springframework.test.context.junit4.SpringRunner;

import static org.assertj.core.api.Assertions.assertThat;

/**
 * Integration tests for demo application.
 *
 * @author hcxin
 * @author Gary Russell
 */
@RunWith(SpringRunner.class)
@SpringBootTest(classes = { SampleKafkaApplicationTests.Config.class, SampleKafkaApplication.class })
@TestPropertySource(properties = "spring.kafka.bootstrap-servers=${spring.embedded.kafka.brokers}")
@EmbeddedKafka
public class SampleKafkaApplicationTests {

	private static final CountDownLatch latch = new CountDownLatch(1);

	@Rule
	public OutputCapture outputCapture = new OutputCapture();

	@Autowired
	private Producer producer;

	@Test
	public void sendSimpleMessage() throws Exception {
		SampleMessage message = new SampleMessage(1, "Test message");
		this.producer.send(message);
		assertThat(latch.await(10, TimeUnit.SECONDS)).isTrue();
		assertThat(this.outputCapture.toString().contains("Test message")).isTrue();
	}

	@Configuration
	public static class Config {

		@Bean
		public static WrapperBPP consumerWrapper() {
			// Wrap the consumer in a proxy and count down the test latch
			return new WrapperBPP();
		}

	}

	private static class WrapperBPP implements BeanPostProcessor, Ordered {

		@Override
		public Object postProcessAfterInitialization(Object bean, String beanName) throws BeansException {
			if (bean instanceof Consumer) {
				MethodInterceptor advice = new MethodInterceptor() {

					@Override
					public Object invoke(MethodInvocation invocation) throws Throwable {
						if (invocation.getMethod().getName().equals("processMessage")) {
							Object proceed = invocation.proceed();
							latch.countDown();
							return proceed;
						}
						return invocation.proceed();
					}

				};
				if (AopUtils.isAopProxy(bean)) {
					((Advised) bean).addAdvice(advice);
					return bean;
				}
				ProxyFactory pf = new ProxyFactory(bean);
				pf.addAdvice(advice);
				return pf.getProxy();
			}
			return bean;
		}

		@Override
		public int getOrder() {
			return Ordered.HIGHEST_PRECEDENCE;
		}

	}

}

