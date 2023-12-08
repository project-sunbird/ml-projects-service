/**
 * name : kafka.js
 * author : Aman Karki
 * created-date : 08-Sep-2020
 * Description : Kafka Configurations related information.
*/


//dependencies
const kafka = require('kafka-node');
const SUBMISSION_TOPIC = process.env.SUBMISSION_TOPIC;
const CERTIFICATE_TOPIC = process.env.PROJECT_SUBMISSION_TOPIC;
const USER_DELETE_TOPIC = process.env.USER_DELETE_TOPIC;
const USER_DELETE_ON_OFF = process.env.USER_DELETE_ON_OFF
/**
  * Kafka configurations.
  * @function
  * @name connect
*/

const connect = function() {

    const Producer = kafka.Producer
    KeyedMessage = kafka.KeyedMessage
    
    const client = new kafka.KafkaClient({
      kafkaHost : process.env.KAFKA_URL
    });

    client.on('error', function(error) {
        console.log("kafka connection error!")
    });

    const producer = new Producer(client)

    producer.on('ready', function () {
        console.log("Connected to Kafka");
    });
   
    producer.on('error', function (err) {
        console.log("kafka producer creation error!")
    })

    _sendToKafkaConsumers(
      SUBMISSION_TOPIC,
      process.env.KAFKA_URL
    );

    //  project certificate details consumer
    _sendToKafkaConsumers(
      CERTIFICATE_TOPIC,
      process.env.KAFKA_URL
    );

    // user Delete Consumer
    if(USER_DELETE_ON_OFF !== "OFF") {
      _sendToKafkaConsumers(USER_DELETE_TOPIC, process.env.KAFKA_URL);
    }

    return {
      kafkaProducer: producer,
      kafkaClient: client
    };

};

/**
  * Send data based on topic to kafka consumers
  * @function
  * @name _sendToKafkaConsumers
  * @param {String} topic - name of kafka topic.
  * @param {String} host - kafka host
*/

var _sendToKafkaConsumers = function (topic,host) {
    
  if (topic && topic != "") {

    let consumer = new kafka.ConsumerGroup(
      {
          kafkaHost : host,
          groupId : process.env.KAFKA_GROUP_ID,
          autoCommit : true
      },topic 
    );  

    consumer.on('message', async function (message) {

      console.log("-------Kafka consumer log starts here------------------");
      console.log("Topic Name: ", topic);
      console.log("Message: ", JSON.stringify(message));
      console.log("-------Kafka consumer log ends here------------------");
  

      if (message && message.topic === SUBMISSION_TOPIC) {
        submissionsConsumer.messageReceived(message);
      }
      // call projectCertificateConsumer 
      if (message && message.topic === CERTIFICATE_TOPIC) {
        projectCertificateConsumer.messageReceived(message);
      }

      // call userDelete consumer
      if (message && message.topic === USER_DELETE_TOPIC) {
        userDeleteConsumer.messageReceived(message);
      }

    });

    consumer.on('error', async function (error) {

      if(error.topics && error.topics[0] === SUBMISSION_TOPIC) {
        submissionsConsumer.errorTriggered(error);
      }
      if(error.topics && error.topics[0] === CERTIFICATE_TOPIC) {
        projectCertificateConsumer.errorTriggered(error);
      }

      if (error.topics && error.topics[0] === USER_DELETE_TOPIC) {
        userDeleteConsumer.errorTriggered(error);
      }

    });

  }
};

module.exports = connect;
