/**
 * name : projectCertificate.js
 * author : Vishnu
 * created-date : 10-Oct-2022
 * Description : Project certificates submission consumer.
*/

//dependencies
const userProjectsHelper = require(MODULES_BASE_PATH + "/userProjects/helper");

/**
* submission consumer message received.
* @function
* @name messageReceived
* @param {String} message - consumer data
* @returns {Promise} return a Promise.
*/

var messageReceived = function (message) {
  return new Promise(async function (resolve, reject) {
    try {
      // This consumer is consuming from an old topic : PROJECT_CERTIFICATE_TOPIC, which is no more used by data team. ie) using existig topic instead of creating new one.
      let parsedMessage = JSON.parse( message.value );
      if ( parsedMessage.status == CONSTANTS.common.SUBMITTED_STATUS &&
           parsedMessage.certificate &&
           Object.keys(parsedMessage.certificate).length > 0 &&
           !parsedMessage.certificate.eligible 
        ) {
          await userProjectsHelper.generateCertificate( parsedMessage );
      }
      return resolve("Message Received");
    } catch (error) {
      return reject(error);
    }

  });
};

/**
* If message is not received.
* @function
* @name errorTriggered
* @param {Object} error - error object
* @returns {Promise} return a Promise.
*/

var errorTriggered = function (error) {
  return new Promise(function (resolve, reject) {

    try {
      return resolve(error);
    } catch (error) {
      return reject(error);
    }

  });
};

module.exports = {
  messageReceived: messageReceived,
  errorTriggered: errorTriggered
};
