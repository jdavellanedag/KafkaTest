# **Workflow Kafka Broker connection**

## **General description**

The following document describes a workflow through a Kafka Broker, it pretends to test a successful connection and a failed connection using the same helper code and supported by a series of tests.

### **Prerequisites**

- Use gradle build tool.
- Download the following source code from: [https://github.com/jdavellanedag/KafkaTest.git](https://github.com/jdavellanedag/KafkaTest.git)
- Within the source code folder run the following command: `gradle build` or `./gradlew build`

### **Launching the testing**

- Within the same source code folder run the following command: `gradle test` or `./gradlew test`
- Within the same source code folder, open the following file \app\build\reports\tests\test\index.html
- Check the test results.

### **Explanation**

There are five tests described below:

**Connection to an invalid broker:** This test expects an exception when an invalid or unreachable broker endpoint is provided. Purpose: This test pretends to check the code behavior in case of an invalid broker.

**Connection to a valid broker:** This test expects to get a successful connection with a broker without any exception. Purposes: This test pretends to check the code behavior in case of providing a valid broker endpoint, establishing a successful connection with this broker.

**Connection to a topic:** This test expects to get a successful subscription to a topic within the broker. Purposes: This test pretends to check the code behavior when the consuming method is used to subscribe to a topic.

**Get data from a topic:** This test expects to check the data collected from a topic. Purpose: This test pretends to validate the method which collects data from the topic and in the same way validates that this data is valid.

**Test an open connection:** This test expects to have an open connection with the broker for a given period of time and then stop the process without having any exception except for the expected one in the shutting down process. Purpose: This test pretends to check the ability of the code to get an uninterrupted connection with the broker and then finishes this connection when it is wanted.

### **Conclusion**

The code is capable of detecting an invalid endpoint and generating a log in case of a failure connection, otherwise it will connect successfully with the broker provided, in addition it is capable for collecting data from the specified topic and establishes a connection for a long time period without any interruption except by shutting down the process when it is needed. The code has proven to work using a valid broker therefore the issue might emerge at another point in the workflow but not during the processing of the data from the consumer.
