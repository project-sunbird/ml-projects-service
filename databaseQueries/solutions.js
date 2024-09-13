/**
 * name : solutions.js
 * author : Vishnu
 * created-date : 26-Jan-2022
 * Description : solutions helper for DB interactions.
 */

// Dependencies 

/**
    * Solutions
    * @class
*/



module.exports= class Solutions{
    /**
     * Solution details.
     * @method
     * @name solutionsDocument
     * @param {Array} [filterData = "all"] - solutions filter query.
     * @param {Array} [fieldsArray = "all"] - projected fields.
     * @param {Array} [skipFields = "none"] - field not to include
     * @returns {Array} solutions details.
     */
    
     static solutionsDocument(
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
               let solutionsDoc = 
               await database.models.solutions.find(
                   queryObject, 
                   projection
               ).lean();
           
               return resolve(solutionsDoc);
           
           } catch (error) {
               return reject(error);
           }
       });
   }

    /**
    * Update solution document.
    * @method
    * @name updateSolutionDocument
    * @param {Object} query - query to find document
    * @param {Object} updateObject - fields to update
    * @returns {String} - message.
    */

   static updateSolutionDocument(query= {}, updateObject= {}) {
    return new Promise(async (resolve, reject) => {
        try {

            if (Object.keys(query).length == 0) {
                throw new Error(messageConstants.apiResponses.UPDATE_QUERY_REQUIRED)
            }

            if (Object.keys(updateObject).length == 0) {
                throw new Error (messageConstants.apiResponses.UPDATE_OBJECT_REQUIRED)
            }

            let updateResponse = await database.models.solutions.updateOne
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
}