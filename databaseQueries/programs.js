/**
 * name : programs.js
 * author : Vishnu
 * created-date : 09-Mar-2022
 * Description : program helper for DB interactions.
 */

// Dependencies 

/**
    * Programs
    * @class
*/



module.exports= class Programs{
    /**
     * programs details.
     * @method
     * @name programsDocument
     * @param {Array} [filterData = "all"] - programs filter query.
     * @param {Array} [fieldsArray = "all"] - projected fields.
     * @param {Array} [skipFields = "none"] - field not to include
     * @returns {Array} program details.
     */

     static programsDocument(
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
               let programsDoc = 
               await database.models.programs.find(
                   queryObject, 
                   projection
               ).lean();
               return resolve(programsDoc);
           } catch (error) {
               return reject(error);
           }
       });
   }
}