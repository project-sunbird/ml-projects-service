/**
 * name : producer.js
 * author : Aman Karki
 * created-date : 08-Sep-2020
 * Description : Kafka Producer related information.
*/

// Dependencies
const kafkaCommunicationsOnOff = (!process.env.KAFKA_COMMUNICATIONS_ON_OFF || process.env.KAFKA_COMMUNICATIONS_ON_OFF != "OFF") ? "ON" : "OFF";
const projectSubmissionTopic = (process.env.PROJECT_SUBMISSION_TOPIC && process.env.PROJECT_SUBMISSION_TOPIC != "OFF") ? process.env.PROJECT_SUBMISSION_TOPIC : "sl-improvement-project-submission-dev";
const telemetryEventTopic = process.env.TELEMETRY_TOPIC ? process.env.TELEMETRY_TOPIC : "dev.telemetry.raw";

/**
  * Push improvement projects to kafka.
  * @function
  * @name pushProjectToKafka
  * @param {Object} message - Message data.
*/

const pushProjectToKafka = function (message) {
  return new Promise(async (resolve, reject) => {
      try {

          let kafkaPushStatus = await pushMessageToKafka([{
            topic: projectSubmissionTopic,
            messages: JSON.stringify(message)
          }]);

          return resolve(kafkaPushStatus);

      } catch (error) {
          return reject(error);
      }
  })
}



/**
 * Push message to telemetry.
 * @function
 * @name pushTelemetryEventToKafka
 * @param {Object} message - Message data.
 */
 const pushTelemetryEventToKafka = function (message) {
  return new Promise(async (resolve, reject) => {
    try {
      let kafkaPushStatus = await pushMessageToKafka([
        {
          topic: telemetryEventTopic,
          messages: JSON.stringify(message),
        },
      ]);
    
      return resolve(kafkaPushStatus);
    } catch (error) {
      return reject(error);
    }
  });
};

/**
  * Push message to kafka.
  * @function
  * @name pushMessageToKafka
  * @param {Object} payload - Payload data.
*/

const pushMessageToKafka = function(payload) {
  return new Promise((resolve, reject) => {

    if (kafkaCommunicationsOnOff != "ON") {
      throw reject("Kafka configuration is not done");
    }

    console.log("-------Kafka producer log starts here------------------");
    console.log("Topic Name: ",  payload[0].topic);
    console.log("Message: ", JSON.stringify(payload));
    console.log("-------Kafka producer log ends here------------------");

    
    kafkaClient.kafkaProducer.send(payload, (err, data) => {
      if (err) {
        return reject("Kafka push to topic "+ payload[0].topic +" failed.");
      } else {
        return resolve(data);
      }
    })

  }).then(result => {
      
    return {
      status : CONSTANTS.common.SUCCESS,
      message: "Kafka push to topic "+ payload[0].topic +" successful with number - "+result[payload[0].topic][0]
    };

  }).catch((err) => {
    return {
      status : "failed",
      message: err
    }
  })
}


module.exports = {
  pushProjectToKafka : pushProjectToKafka,
  pushTelemetryEventToKafka : pushTelemetryEventToKafka
};

