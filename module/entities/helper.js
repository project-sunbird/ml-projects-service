/**
 * name : helper.js
 * author : Ankit Shahu
 * created-date : 16-Jan-2024
 * Description : entities helper functionality.
 */
 
const userProfileService = require(GENERICS_FILES_PATH + "/services/users");

module.exports = class entitieHelper {
 
 /**
   * update registry in entities.
   * @method
   * @name listByLocationIds
   * @param {Object} locationIds - locationIds
   * @returns {Object} entity Document
   */

    static listByLocationIds(locationIds) {
        return new Promise(async (resolve, reject) => {
            try {
                //if not uuid considering as location code- for school.
                let locationDeatails = UTILS.filterLocationIdandCode(locationIds,);
                //set request body for learners api
                let entityInformation = [];
                let formatResult = true;
            
                if ( locationDeatails.ids.length > 0 ) {
                    let bodyData = {
                        "id" : locationDeatails.ids
                    } 
                    let entityData = await userProfileService.locationSearch( bodyData, formatResult );
                    if ( entityData.success ) {
                        entityInformation =  entityData.data;
                    }
                }

                if ( locationDeatails.codes.length > 0 ) {
                    let bodyData = {
                        "code" : locationDeatails.codes
                    } 
                    let entityData = await userProfileService.locationSearch( bodyData, formatResult );
                    if ( entityData.success ) {
                        entityInformation =  entityInformation.concat(entityData.data);
                    }
                }
            
                if ( !entityInformation.length > 0 ) {
                    throw {
                        message : CONSTANTS.apiResponses.ENTITY_NOT_FOUND
                    } 
                }
                
                return resolve({
                    success : true,
                    message : CONSTANTS.apiResponses.ENTITY_FETCHED,
                    data : entityInformation
                });

            } catch(error) {
                return resolve({
                    success : false,
                    message : error.message
                });
            }
        })
    }

}