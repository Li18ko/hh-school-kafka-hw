package ru.hh.kafkahw;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.slf4j.Logger;
import org.slf4j.LoggerFactory;
import org.springframework.beans.factory.annotation.Autowired;
import org.springframework.kafka.annotation.KafkaListener;
import org.springframework.kafka.support.Acknowledgment;
import org.springframework.stereotype.Component;
import ru.hh.kafkahw.internal.Service;
import ru.hh.kafkahw.internal.KafkaProducer;

@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;

  @Autowired
  public TopicListener(Service service) {
    this.service = service;
  }

  @KafkaListener(topics = "topic1", groupId = "group1")
  public void atMostOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    /* atMostOnce - гарантирует, что сообщение будет доставлено один раз или вообще не будет доставлено.
    Т.е. достаточно отправить один раз, а там уже оно либо дойдет, либо потеряется по пути.
    Делаем попытку отправить только если service.count("topic1", consumerRecord.value() равен нулю (меньше 1).
    (https://habr.com/ru/companies/otus/articles/811479/)*/
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    if(service.count("topic1", consumerRecord.value()) < 1){
      try{
        service.handle("topic1", consumerRecord.value());
      }
      finally {
        ack.acknowledge();
      }
    }
  }

  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    int countError = 0;
    int maxCountError = 10;
    /*atLeastOnce - гарантирует, что сообщение будет доставлено по крайней мере один раз.
    Пока количество будет равно нулю, мы будем отправлять заново.
    Подтверждаем доставку и убираем из очереди только при благоприятных условиях отправки.
    + Ставлю ограничение на количество попыток, чтобы не уйти в бесконечный цикл*/
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    while (service.count("topic2", consumerRecord.value()) == 0 && countError < maxCountError){
      try{
        service.handle("topic2", consumerRecord.value());
        ack.acknowledge();
      }catch(Exception ex){
        countError++;
      }
    }
  }

  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    int countError = 0;
    int maxCountError = 10;
    /* exactlyOnce - гарантирует, что каждое сообщение будет отправлено ровно 1 раз,
    что исключает возможность потери или дублирования данных.
    Т.е. пробуем отправить сообщение до момента, пока количество не будет равно 1.
    Как только количество равно 1, мы выходим из цикла.
    Также ограничение на количество попыток, чтобы не уйти в бесконечный цикл*/
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    while(service.count("topic3", consumerRecord.value()) != 1 && countError < maxCountError){
      try{
        service.handle("topic3", consumerRecord.value());
        ack.acknowledge();
      }catch(Exception ex){
        countError++;
      }
    }
  }
}
