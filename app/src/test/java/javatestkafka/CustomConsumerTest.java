package javatestkafka;

import static org.junit.jupiter.api.Assertions.assertDoesNotThrow;
import static org.junit.jupiter.api.Assertions.assertEquals;
import static org.junit.jupiter.api.Assertions.assertFalse;
import static org.junit.jupiter.api.Assertions.assertThrows;
import static org.junit.jupiter.api.Assertions.assertTrue;

import java.util.Collections;
import java.util.HashMap;

import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.KafkaConsumer;
import org.apache.kafka.clients.consumer.MockConsumer;
import org.apache.kafka.clients.consumer.OffsetResetStrategy;
import org.apache.kafka.common.KafkaException;
import org.apache.kafka.common.TopicPartition;
import org.apache.kafka.common.errors.WakeupException;
import org.junit.jupiter.api.BeforeEach;
import org.junit.jupiter.api.DisplayName;
import org.junit.jupiter.api.Nested;
import org.junit.jupiter.api.Test;

class CustomConsumerTest {

  final String TOPIC = "TOPIC";
  final String BROKERS = "bad.server:4040";
  final int SECONDS = 5;

  MockConsumer<String, String> consumer;
  KafkaConsumer<String, String> consumerKafka;
  CustomConsumer<String, String> customConsumer;

  @BeforeEach
  void setUp() {
    this.customConsumer = new CustomConsumer<String, String>();
    this.consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    this.customConsumer.createConsumer(this.consumer);
    this.consumer.schedulePollTask(() -> {
      this.consumer.rebalance(Collections.singletonList(new TopicPartition(this.TOPIC, 0)));
      this.consumer.addRecord(new ConsumerRecord<String, String>(this.TOPIC, 0, 0L, "key", "value"));
    });
    this.consumer.schedulePollTask(() -> this.customConsumer.stop());
    HashMap<TopicPartition, Long> startOffset = new HashMap<>();
    TopicPartition tp = new TopicPartition(this.TOPIC, 0);
    startOffset.put(tp, 0L);
    this.consumer.updateBeginningOffsets(startOffset);
  }

  /**
   * Test connection with an unreachable broker
   */
  @Test
  @DisplayName("Test connection with a invalid broker, it should raise an exception")
  void connectionToUnreachableBroker() {
    assertThrows(KafkaException.class, () -> {
      this.customConsumer = new CustomConsumer<String, String>(BROKERS);
      this.consumerKafka = new KafkaConsumer<>(this.customConsumer.getProps());
    });
  }

  /**
   * Test the connection with broker
   */
  @Test
  @DisplayName("Test connection with valid broker, it should not raise any exception")
  void connectionToBroker() {
    assertDoesNotThrow(() -> {
      this.customConsumer = new CustomConsumer<String, String>();
      this.consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
    });
  }

  /**
   * Test a subscription with a topic
   */
  @Test
  @DisplayName("Test subscription to a topic")
  void connectionToTopic() {
    assertDoesNotThrow(() -> {
      this.customConsumer.consume(this.TOPIC, this.SECONDS);
    });
  }

  /**
   * Test collecting valid data from the topic
   */
  @Test
  @DisplayName("Test collectiong valid data from a topic")
  void consumeDataFromTopic() {
    assertDoesNotThrow(() -> {
      this.customConsumer.consume(this.TOPIC, this.SECONDS);
    });
    assertFalse(this.customConsumer.getData().isEmpty());
    assertEquals("value", this.customConsumer.getData().get(0));
  }

  /**
   * Test an open connection with the broker and then shutdown process
   */
  @Nested
  class WhenRunning {

    @BeforeEach
    void setUp() {
      customConsumer = new CustomConsumer<String, String>();
      consumer = new MockConsumer<>(OffsetResetStrategy.EARLIEST);
      customConsumer.createConsumer(consumer);
      assertDoesNotThrow(() -> {
        customConsumer.consume(TOPIC, SECONDS);
      });
    }

    @Test
    @DisplayName("Test open a connection for a given time and then shut down the process")
    void openConnectionToBroker() {

      assertThrows(WakeupException.class, () -> {
        customConsumer.stop();
      });
      assertTrue(consumer.closed());
    }
  }
}
