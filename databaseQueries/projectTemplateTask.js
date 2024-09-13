/**
 * name : projectTemplateTask.js
 * author : Priyanka
 * created-date : 01-Sep-2021
 * Description : Project Templates helper for DB interactions.
 */

// Dependencies 

/**
    * ProjectTemplateTask
    * @class
*/

module.exports = class ProjectTemplateTask {

    /**
     * Lists of template tasks.
     * @method
     * @name taskDocuments
     * @param {Array} [filterData = "all"] - template filter query.
     * @param {Array} [fieldsArray = "all"] - projected fields.
     * @param {Array} [skipFields = "none"] - field not to include
     * @returns {Array} Lists of template. 
     */
    
    static taskDocuments(
        filterData = "all", 
        fieldsArray = "all",
        skipFields = "none"
    ) {
        return new Promise(async (resolve, reject) => {
            try {
                
                let queryObject = (filterData != "all") ? filterData : {};
                let projection = {}
           
                if (fieldsArray != "all") {
                    fieldsArray.forEach(field => {
                        projection[field] = 1;
                   });
               }
               
               if( skipFields !== "none" ) {
                   skipFields.forEach(field=>{
                       projection[field] = 0;
                   });
               }
               
               let templateTasks = 
               await database.models.projectTemplateTasks.find(
                   queryObject, 
                   projection
               ).lean();
           
               return resolve(templateTasks);
           
           } catch (error) {
               return reject(error);
           }
       });
   }

   /**
   * Create project template task.
   * @method
   * @name createTemplateTask
   * @param {Object} [templateData] - template task Data.
   * @returns {Object} - Project template task data.
   */

    static createTemplateTask(templateData) {
        return new Promise(async (resolve, reject) => {
        
            try {
              
              let templateTask = await database.models.projectTemplateTasks.create(templateData);
              return resolve(templateTask);

            } catch (error) {
              return reject(error);
            }
        });
    }

    /**
    * Update projectTemplateTask document.
    * @method
    * @name updateTaskDocument
    * @param {Object} query - query to find document
    * @param {Object} updateObject - fields to update
    * @returns {String} - message.
    */

   static updateTaskDocument(query= {}, updateObject= {}) {
        return new Promise(async (resolve, reject) => {
            try {

                if (Object.keys(query).length == 0) {
                    throw new Error(CONSTANTS.apiResponses.UPDATE_QUERY_REQUIRED)
                }

                if (Object.keys(updateObject).length == 0) {
                    throw new Error (CONSTANTS.apiResponses.UPDATE_OBJECT_REQUIRED)
                }

                let updateResponse = await database.models.projectTemplateTasks.updateOne
                (
                    query,
                    updateObject
                )
                
                if (updateResponse.nModified == 0) {
                    throw new Error(CONSTANTS.apiResponses.FAILED_TO_UPDATE)
                }

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.UPDATED_DOCUMENT_SUCCESSFULLY,
                    data: true
                });

            } catch (error) {
                return resolve({
                    success: false,
                    message: error.message,
                    data: false
                });
            }
        });
   }

   /**
   * Update project templates task documents.
   * @method
   * @name findOneAndUpdate
   * @param {Object} [filterQuery] - filtered Query.
   * @param {Object} [updateData] - update data.
   * @returns {Object} - Project templates task data.
   */

    static findOneAndUpdate(findQuery,UpdateObject, returnData = {}) {
        return new Promise(async (resolve, reject) => {
        
            try {
              
              let templateTask = await database.models.projectTemplateTasks.findOneAndUpdate(findQuery,UpdateObject, returnData);
              return resolve(templateTask);

            } catch (error) {
              return reject(error);
            }
        });
    }

};
