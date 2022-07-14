package javatestkafka;

import java.time.Duration;
import java.util.ArrayList;
import java.util.Arrays;
import java.util.Properties;
import java.util.UUID;

import org.apache.kafka.clients.consumer.Consumer;
import org.apache.kafka.clients.consumer.ConsumerConfig;
import org.apache.kafka.clients.consumer.ConsumerRecord;
import org.apache.kafka.clients.consumer.ConsumerRecords;
import org.apache.kafka.common.errors.WakeupException;
import org.apache.kafka.common.serialization.IntegerDeserializer;
import org.apache.kafka.common.serialization.StringDeserializer;

/**
 * Class for consuming data from a Kafka Server
 * 
 * @author Julian Avellaneda
 * 
 */
public class CustomConsumer<K, V> {

  // Stores the configuration for the broker connection
  private Properties props;

  // Generic consumer to implement within the class
  private Consumer<K, V> consumer;

  // Stores the data retrieved from the Kafka Server
  private ArrayList<V> data;

  /**
   * Create the consumer without using a broker server address
   */
  public CustomConsumer() {
    this.data = new ArrayList<>();
  }

  /**
   * Create the consumer using a broker server address
   * 
   * @param brokers List of brokers separated by comma
   */
  public CustomConsumer(String brokers) {

    props = new Properties();
    props.put(ConsumerConfig.BOOTSTRAP_SERVERS_CONFIG, brokers);
    props.put(ConsumerConfig.GROUP_ID_CONFIG, UUID.randomUUID().toString());
    props.put(ConsumerConfig.AUTO_OFFSET_RESET_CONFIG, "earliest");
    props.put(ConsumerConfig.KEY_DESERIALIZER_CLASS_CONFIG, IntegerDeserializer.class);
    props.put(ConsumerConfig.VALUE_DESERIALIZER_CLASS_CONFIG, StringDeserializer.class);
    this.data = new ArrayList<>();

  }

  /**
   * Set the consumer type, it might be KafkaConsumer or MockConsumer
   * 
   * @param consumer Type of consumer to be setted
   */
  public void createConsumer(Consumer<K, V> consumer) {
    this.consumer = consumer;
  }

  /**
   * Consume data from a Topic
   * 
   * @param topic   Topic name for getting data
   * @param seconds Max time to block
   */
  public void consume(String topic, int seconds) throws Exception {
    try {
      this.consumer.subscribe(Arrays.asList(topic));
      while (true) {
        ConsumerRecords<K, V> records = this.consumer.poll(Duration.ofSeconds(seconds));
        for (ConsumerRecord<K, V> record : records) {
          data.add(record.value());
        }
        this.consumer.commitAsync();
      }
    } catch (WakeupException e) {
      System.out.println("Shutting down");
    } catch (Exception e) {
      System.out.println(e.getMessage());
    } finally {
      this.consumer.close();
    }
  }

  /**
   * Get the values collected from record within the broker
   * 
   * @return Array with values
   */
  public ArrayList<V> getData() {
    return this.data;
  }

  /**
   * Get the configuration properties
   * 
   * @return Config properties
   */
  public Properties getProps() {
    return this.props;
  }

  /**
   * Stop/abort the current polling operation
   * 
   */
  public void stop() {
    this.consumer.wakeup();
  }

}
