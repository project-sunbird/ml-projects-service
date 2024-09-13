/**
 * name : submissions.js
 * author : Aman Jung Karki
 * created-date : 22-Nov-2020
 * Description : Submission consumer.
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
      
      let parsedMessage = JSON.parse(message.value);
      
      let submissionDocument = {
        "_id" : parsedMessage._id.toString(),
        "status" : parsedMessage.status,
        "completedDate" : parsedMessage.submissionDate ? parsedMessage.submissionDate : ""
      };

      await userProjectsHelper.pushSubmissionToTask(
        parsedMessage.projectId,
        parsedMessage.taskId,
        submissionDocument
      );

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
