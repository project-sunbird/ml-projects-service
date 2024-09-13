/**
 * name : helper.js
 * author : Rakesh
 * created-date : 11-Jun-2020
 * Description : DataPipeline helper functionality.
 */

// Dependencies

const UserProjectsHelper = require(MODULES_BASE_PATH + "/userProjects/helper");

/**
 * dataPipelineHelper
 * @class
 */

module.exports = class dataPipelineHelper {

    /**
     * get uset project details.
     * @method
     * @name userProject 
     * @param {String} projectId - project id.
     * @returns {Object} Project details.
     */

    static userProject(projectId) {
        return new Promise(async (resolve, reject) => {
            try {

                const projectDetails = await UserProjectsHelper.userProject(projectId);
                return resolve(projectDetails);

            } catch (error) {
                return resolve({
                    success: false,
                    message: error.message,
                    data: {}
                });
            }
        })
    }

}