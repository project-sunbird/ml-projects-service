/**
 * name : projectTemplates.js
 * author : Priyanka
 * created-date : 01-Sep-2021
 * Description : Project Templates helper for DB interactions.
 */

// Dependencies 

/**
    * ProjectTemplates
    * @class
*/

module.exports = class ProjectTemplates {

    /**
   * Project templates documents.
   * @method
   * @name getAggregate
   * @param {Object} [aggregateData] - aggregate Data.
   * @returns {Array} - Project templates data.
   */

    static getAggregate(aggregateData) {
        return new Promise(async (resolve, reject) => {
        
            try {
              
              let projectTemplatesData = await database.models.projectTemplates.aggregate(aggregateData);
              return resolve(projectTemplatesData);

            } catch (error) {
              return reject(error);
            }
        });
    }

    /**
     * Lists of template.
     * @method
     * @name templateDocument
     * @param {Array} [filterData = "all"] - template filter query.
     * @param {Array} [fieldsArray = "all"] - projected fields.
     * @param {Array} [skipFields = "none"] - field not to include
     * @returns {Array} Lists of template. 
     */
    
    static templateDocument(
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
               let templates = 
               await database.models.projectTemplates.find(
                   queryObject, 
                   projection
               ).lean();
           
               return resolve(templates);
           
           } catch (error) {
               return reject(error);
           }
       });
   }

   /**
   * Create project templates documents.
   * @method
   * @name createTemplate
   * @param {Object} [templateData] - template Data.
   * @returns {Array} - Project templates data.
   */

    static createTemplate(templateData) {
        return new Promise(async (resolve, reject) => {
        
            try {
              
              let projectTemplate = await database.models.projectTemplates.create(templateData);
              return resolve(projectTemplate);

            } catch (error) {
              return reject(error);
            }
        });
    }

    /**
   * Update project templates documents.
   * @method
   * @name findOneAndUpdate
   * @param {Object} [filterQuery] - filtered Query.
   * @param {Object} [updateData] - update data.
   * @returns {Array} - Project templates data.
   */

    static findOneAndUpdate(findQuery,UpdateObject, returnData = {}) {
        return new Promise(async (resolve, reject) => {
        
            try {
              
              let projectTemplate = await database.models.projectTemplates.findOneAndUpdate(findQuery,UpdateObject, returnData);
              return resolve(projectTemplate);

            } catch (error) {
              return reject(error);
            }
        });
    }

     /**
    * Update projectTemplates document.
    * @method
    * @name updateProjectTemplateDocument
    * @param {Object} query - query to find document
    * @param {Object} updateObject - fields to update
    * @returns {String} - message.
    */

   static updateProjectTemplateDocument(query= {}, updateObject= {}) {
    return new Promise(async (resolve, reject) => {
        try {

            if (Object.keys(query).length == 0) {
                throw new Error(CONSTANTS.apiResponses.UPDATE_QUERY_REQUIRED)
            }

            if (Object.keys(updateObject).length == 0) {
                throw new Error (CONSTANTS.apiResponses.UPDATE_OBJECT_REQUIRED)
            }

            let updateResponse = await database.models.projectTemplates.updateOne
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

};
