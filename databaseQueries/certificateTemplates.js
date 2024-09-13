/**
 * name : certificateTemplates.js
 * author : Vishnu
 * created-date : 03-Oct-2022
 * Description : Certificate template helper for DB interactions.
 */

// Dependencies 

/**
    * CertificateTemplates
    * @class
*/

module.exports= class CertificateTemplates{
    /**
     * certificate template details.
     * @method
     * @name certificateTemplateDocument
     * @param {Array} [filterData = "all"] - certificate template filter query.
     * @param {Array} [fieldsArray = "all"] - projected fields.
     * @param {Array} [skipFields = "none"] - field not to include
     * @returns {Array} certificateTemplates details.
    */
    
    static certificateTemplateDocument(
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
               let certificateTemplateDoc = 
               await database.models.certificateTemplates.find(
                   queryObject, 
                   projection
               ).lean();
           
               return resolve(certificateTemplateDoc);
           
            } catch (error) {
                return reject(error);
            }
        });
    }
}