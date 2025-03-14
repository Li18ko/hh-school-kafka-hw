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

import java.util.Collections;
import java.util.Set;
import java.util.UUID;
import java.util.concurrent.ConcurrentHashMap;


@Component
public class TopicListener {
  private final static Logger LOGGER = LoggerFactory.getLogger(TopicListener.class);
  private final Service service;

  private Set<UUID> processedMessages = Collections.newSetFromMap(new ConcurrentHashMap<>());

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

    /*UPD: если у нас нет доступа к данным сервиса, то мы и правда можем сначала акнуть
    и затем уже отправить сообщение, что никак не нарушит принцип atMostOnce.
    Подтверждаем, что сообщение получено, а затем уже отправляем 1 раз
    (либо нормально обработается и дойдет, либо упадет и вообще не будет доставлено).
    Таким образом подтверждение отправки не зависит от окончания обработки сообщения с ошибкой или без*/

    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    ack.acknowledge();
    try{
      service.handle("topic1", consumerRecord.value());
    }catch (Exception ex){
    }
  }

  @KafkaListener(topics = "topic2", groupId = "group2")
  public void atLeastOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    /*atLeastOnce - гарантирует, что сообщение будет доставлено по крайней мере один раз.
    Пока количество будет равно нулю, мы будем отправлять заново.
    Подтверждаем доставку и убираем из очереди только при благоприятных условиях отправки.
    + Ставлю ограничение на количество попыток, чтобы не уйти в бесконечный цикл*/

    /*UPD: Можно сначала обработать, а потом уже акнуть. Тогда у нас будет гарантия,
    что акнется точно после успешной отправки и обработки, и что у нас не упадет обработка
    в момент, когда ак уже сделали. Но я думала сделать ограниченное количество попыток отправки сообщения
    для того, чтобы мы не ушли в бесконечный цикл. Но опять же там использовала каунт, из прошлого комментария поняла,
    что у нас нет данных другого сервиса и мы не можем его использовать*/

    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    try {
      service.handle("topic2", consumerRecord.value());
      ack.acknowledge();
    } catch (Exception ex) {
    }
  }

  @KafkaListener(topics = "topic3", groupId = "group3")
  public void exactlyOnce(ConsumerRecord<?, String> consumerRecord, Acknowledgment ack) {
    /* exactlyOnce - гарантирует, что каждое сообщение будет отправлено ровно 1 раз,
    что исключает возможность потери или дублирования данных.
    Т.е. пробуем отправить сообщение до момента, пока количество не будет равно 1.
    Как только количество равно 1, мы выходим из цикла.
    Также ограничение на количество попыток, чтобы не уйти в бесконечный цикл

    UPD: честный хеш используется из преобразования значения сообщения в байты.
    Создала множество уникальных UUID. Если попадается индивидуальный, который не был еще в нашем сете,
    то мы идем отправлять и акать*/
    LOGGER.info("Try handle message, topic {}, payload {}", consumerRecord.topic(), consumerRecord.value());
    UUID messageUUID = UUID.nameUUIDFromBytes(consumerRecord.value().getBytes());
    LOGGER.info("Generated UUID: {}", messageUUID);

    if (processedMessages.add(messageUUID)) {
      LOGGER.info("Message not processed before, handling message: {}", consumerRecord.value());
      try {
        service.handle("topic3", consumerRecord.value());
        ack.acknowledge();
      } catch (Exception ex) {
      }
    } else {
      LOGGER.info("Message already processed, skipping: {}", consumerRecord.value());
    }

  }
}
