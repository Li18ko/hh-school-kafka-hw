package ru.hh.kafkahw.internal;

import java.util.Random;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.kafka.core.KafkaTemplate;
import org.springframework.stereotype.Component;

@Component
public class KafkaProducer {
  private final static Logger LOGGER = LoggerFactory.getLogger(KafkaProducer.class);
  private final Random random = new Random();

  private final KafkaTemplate<String, String> kafkaTemplate;

  public KafkaProducer(KafkaTemplate<String, String> kafkaTemplate) {
    this.kafkaTemplate = kafkaTemplate;
  }

  public void send(String topic, String payload) {
    if (random.nextInt(100) < 10) {
      throw new RuntimeException();
    }
    LOGGER.info("send to kafka, topic {}, payload {}", topic, payload);
    kafkaTemplate.send(topic, payload);
    if (random.nextInt(100) < 2) {
      throw new RuntimeException();
    }
  }
}
