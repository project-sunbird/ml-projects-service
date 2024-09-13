/**
 * name : dataPipeline.js
 * author : Rakesh
 * created-date : 20-July-2020
 * Description : DataPipeline related information.
 */

// Dependencies
const dataPipelineHelper = require(MODULES_BASE_PATH + "/dataPipeline/helper");

 /**
    * DataPipeline
    * @class
*/

module.exports = class DataPipeline {

    /**
     * @apiDefine errorBody
     * @apiError {String} status 4XX,5XX
     * @apiError {String} message Error
     */

    /**
     * @apiDefine successBody
     * @apiSuccess {String} status 200
     * @apiSuccess {String} result Data
     */

    static get name() {
        return "dataPipeline";
    }

    /**
      * Get project details.
      * @method
      * @name read
      * @param {Object} req - request data.
      * @param {String} req.params._id - project Id.
      * @returns {JSON} project details
     */

    async userProject(req) {
            return new Promise(async (resolve, reject) => {
                try {
      
                    const projectDetails = await dataPipelineHelper.userProject(
                        req.params._id
                    );
    
                    return resolve({
                        status: projectDetails.status,
                        message: projectDetails.message,
                        result: projectDetails.data
                    });
    
                } catch (error) {
                    return reject({
                        status: error.status || HTTP_STATUS_CODE.internal_server_error.status,
                        message: error.message || HTTP_STATUS_CODE.internal_server_error.message,
                        errorObject: error
                    });
                }
            })
        }

};