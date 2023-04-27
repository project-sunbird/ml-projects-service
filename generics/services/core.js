/**
 * name : kendra.js
 * author : Aman Jung Karki
 * Date : 16-July-2020
 * Description : Assessment service related information.
 */

//dependencies
const request = require('request');
const fs = require("fs");

const ML_CORE_URL = process.env.ML_CORE_SERVICE_URL;

/**
  * Get downloadable file.
  * @function
  * @name getDownloadableUrl
  * @param {Object} bodyData - body data.
  * @returns {Array} Downloadable file.
*/

const getDownloadableUrl = function (bodyData) {

    let fileDownloadUrl = ML_CORE_URL + CONSTANTS.endpoints.FILES_DOWNLOADABLE_URL;

    console.log("File Downloadable url is",fileDownloadUrl);

    return new Promise((resolve, reject) => {
        try {

            const kendraCallBack = function (err, data) {

                let result = {
                    success : true
                };

                if (err) {
                    console.log("File download error is",err);
                    result.success = false;
                } else {
                    let response = data.body;
                    console.log("File downloaded response is",response);

                    if( response.status === HTTP_STATUS_CODE['ok'].status ) {
                        result["data"] = response.result;
                    } else {
                        result.success = false;
                    }
                }

                return resolve(result);
            }

            request.post(fileDownloadUrl, {
                headers: {
                    "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN
                },
                json : bodyData
            }, kendraCallBack);

        } catch (error) {
            return reject(error);
        }
    })

}

/**
  * List of entity types.
  * @function
  * @name entityTypesDocuments
  * @param {Object} filterData - Filter data.
  * @param {Array} projection - Projected data. 
  * @param {Array} skipFields - Field to skip.  
  * @returns {JSON} - List of entity types.
*/

const entityTypesDocuments = function ( 
    filterData =  "all",
    projection = "all",
    skipFields = "none"
) {
    return new Promise(async (resolve, reject) => {
        try {
            
            const url = ML_CORE_URL + CONSTANTS.endpoints.LIST_ENTITY_TYPES;

            const options = {
                headers : {
                    "content-type": "application/json",
                    "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN,
                },
                json : {
                    query : filterData,
                    projection : projection,
                    skipFields : skipFields
                }
            };

            request.post(url,options,kendraCallback);

            function kendraCallback(err, data) {

                let result = {
                    success : true
                };

                if (err) {
                    result.success = false;
                } else {

                    let response = data.body;

                    if( response.status === HTTP_STATUS_CODE['ok'].status ) {
                        result["data"] = response.result;
                    } else {
                        result.success = false;
                    }
                }

                return resolve(result);
            }

        } catch (error) {
            return reject(error);
        }
    })
}

/**
  * List of roles data.
  * @function
  * @name rolesDocuments
  * @param {Object} filterData - Filter data.
  * @param {Array} projection - Projected data. 
  * @param {Array} skipFields - Field to skip.  
  * @returns {JSON} - List of roles data.
*/

const rolesDocuments = function ( 
    filterData =  "all",
    projection = "all",
    skipFields = "none"
) {
    return new Promise(async (resolve, reject) => {
        try {
            
            const url = ML_CORE_URL + CONSTANTS.endpoints.LIST_USER_ROLES;

            const options = {
                headers : {
                    "content-type": "application/json",
                    "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN,
                },
                json : {
                    query : filterData,
                    projection : projection,
                    skipFields : skipFields
                }
            };

            request.post(url,options,kendraCallback);

            function kendraCallback(err, data) {

                let result = {
                    success : true
                };

                if (err) {
                    result.success = false;
                } else {
                    let response = data.body;

                    if( response.status === HTTP_STATUS_CODE['ok'].status ) {
                        result["data"] = response.result;
                    } else {
                        result.success = false;
                    }
                }

                return resolve(result);
            }

        } catch (error) {
            return reject(error);
        }
    })
}

/**
  * Form details.
  * @function
  * @name formDetails
  * @param {Object} formName - name of the form 
  * @returns {JSON} - Details of the form.
*/

const formDetails = function ( formName ) {
    return new Promise(async (resolve, reject) => {
        try {
            
            const url = 
            ML_CORE_URL + 
            CONSTANTS.endpoints.DETAILS_FORM + "/" + formName;

            const options = {
                headers : {
                    "content-type": "application/json",
                    "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN,
                }
            };

            request.get(url,options,kendraCallback);

            function kendraCallback(err, data) {

                let result = {
                    success : true
                };

                if (err) {
                    result.success = false;
                } else {

                    let response = JSON.parse(data.body);
                    if( response.status === HTTP_STATUS_CODE['ok'].status ) {
                        result["data"] = response.result;
                    } else {
                        result.success = false;
                    }
                }
                return resolve(result);
            }

        } catch (error) {
            return reject(error);
        }
    })
}

/**
  * List of entity data.
  * @function
  * @name entityDocuments
  * @param {Object} filterData - Filter data.
  * @param {Array} projection - Projected data. 
  * @returns {JSON} - List of entity data.
*/

const entityDocuments = function ( 
    filterData =  "all",
    projection = "all"
) {
    return new Promise(async (resolve, reject) => {
        try {
            
            const url = ML_CORE_URL + CONSTANTS.endpoints.LIST_ENTITIES;

            const options = {
                headers : {
                    "content-type": "application/json",
                    "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN,
                },
                json : {
                    entities : filterData,
                    fields : projection
                }
            };

            request.post(url,options,kendraCallback);

            function kendraCallback(err, data) {

                let result = {
                    success : true
                };

                if (err) {
                    result.success = false;
                } else {
                    
                    let response = data.body;
                    if( response.status === HTTP_STATUS_CODE['ok'].status ) {
                        result["data"] = response.result;
                    } else {
                        result.success = false;
                    }
                }

                return resolve(result);
            }

        } catch (error) {
            return reject(error);
        }
    })
}

/**
  * Create user program and solution.
  * @function
  * @name createUserProgramAndSolution
  * @param {string} userId - logged in user id.
  * @param {Object} data - Program and solution creation data. 
  * @returns {JSON} - Create user program and solution.
*/

const createUserProgramAndSolution = function ( data,userToken, createADuplicateSolution = "" ) {
    return new Promise(async (resolve, reject) => {
        try {
            
            let url = ML_CORE_URL + CONSTANTS.endpoints.CREATE_PROGRAM_AND_SOLUTION;

            if( createADuplicateSolution == false ) {
                url = url + "?createADuplicateSolution=" + true;
            }

            const options = {
                headers : {
                    "content-type": "application/json",
                    "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN,
                    "X-authenticated-user-token" : userToken
                },
                json : data
            };

            request.get(url,options,kendraCallback);

            function kendraCallback(err, data) {

                let result = {
                    success : true
                };

                if (err) {
                    result.success = false;
                } else {

                    let response = data.body;
                    if( response.status === HTTP_STATUS_CODE['ok'].status ) {
                        result["data"] = response.result;
                    } else {
                        result.success = false;
                    }
                }

                return resolve(result);
            }

        } catch (error) {
            return reject(error);
        }
    })
}

/**
  * User extension get profile.
  * @function
  * @name getProfile
  * @param {String} token - Logged in user token.
  * @returns {JSON} - User extension get profile.
*/

const getProfile = function ( token ) {
    return new Promise(async (resolve, reject) => {
        try {
            
            const url = ML_CORE_URL + CONSTANTS.endpoints.USER_EXTENSION_GET_PROFILE;

            const options = {
                headers : {
                    "content-type": "application/json",
                    "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN,
                    "x-authenticated-user-token" : token
                }
            };

            request.get(url,options,kendraCallback);

            function kendraCallback(err, data) {

                let result = {
                    success : true
                };

                if (err) {
                    result.success = false;
                } else {
                    
                    let response = JSON.parse(data.body);

                    if( response.status === HTTP_STATUS_CODE['ok'].status ) {
                        result["data"] = response.result;
                    } else {
                        result.success = false;
                    }
                }

                return resolve(result);
            }

        } catch (error) {
            return reject(error);
        }
    })
}

/**
  * Update user rating.
  * @function
  * @name updateUserProfile
  * @param {String} token - Logged in user token.
  * @param {Object} updateData - Update data.
  * @returns {JSON} - Update user rating.
*/

const updateUserProfile = function ( token,updateData ) {
    return new Promise(async (resolve, reject) => {
        try {
            
            const url = ML_CORE_URL + 
            CONSTANTS.endpoints.USER_EXTENSION_UPDATE_USER_PROFILE;

            const options = {
                headers : {
                    "content-type": "application/json",
                    "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN,
                    "x-authenticated-user-token" : token
                },
                json : updateData
            };

            request.post(url,options,kendraCallback);

            function kendraCallback(err, data) {

                let result = {
                    success : true
                };

                if (err) {
                    result.success = false;
                } else {
                    result["data"] = data.body;
                }

                return resolve(result);            }

        } catch (error) {
            return reject(error);
        }
    })
}

/**
  * List of user private programs.
  * @function
  * @name userPrivatePrograms
  * @param {String} token - Logged in user token.
  * @param {String} userId - Logged in user id.
  * @returns {JSON} - List of user private programs.
*/

const userPrivatePrograms = function ( token ) {
    return new Promise(async (resolve, reject) => {
        try {
            
            const url = ML_CORE_URL + 
            CONSTANTS.endpoints.USER_PRIVATE_PROGRAMS;

            const options = {
                headers : {
                    "content-type": "application/json",
                    "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN,
                    "x-authenticated-user-token" : token
                }
            };

            request.post(url,options,kendraCallback);

            function kendraCallback(err, data) {

                let result = {
                    success : true
                };

                if (err) {
                    result.success = false;
                } else {
                    result["data"] = JSON.parse(data.body).result;
                }

                return resolve(result);
            }

        } catch (error) {
            return reject(error);
        }
    })
}

/**
  * Get list of users by entity and role.
  * @function
  * @name getUsersByEntityAndRole
  * @param {String} entityId - entity id.
  * @param {String} role - role. 
  * @returns {JSON} - List of users and entityId.
*/

const getUsersByEntityAndRole = function ( 
   entityId = "",
   role = ""
) {
    return new Promise(async (resolve, reject) => {
        try {
            
            const url = ML_CORE_URL + CONSTANTS.endpoints.GET_USERS_BY_ENTITY_AND_ROLE + "/" + entityId + "?role=" + role;
           
            const options = {
                headers : {
                    "content-type": "application/json",
                    "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN
                }
            };

            request.post(url,options,kendraCallback);

            function kendraCallback(err, data) {

                let result = {
                    success : true
                };

                if (err) {
                    result.success = false;
                } else {
                   
                    let response = JSON.parse(data.body);

                    if( response.status === HTTP_STATUS_CODE['ok'].status ) {
                        result["data"] = response.result;
                    } else {
                        result.success = false;
                    }
                }

                return resolve(result);
            }

        } catch (error) {
            return reject(error);
        }
    })
}

/**
  * Create improvement project solution.
  * @function
  * @name createSolution
  * @param {Object} bodyData - requested body data. 
  * @param {String} token - requested user token. 
  * @returns {JSON} - Improvement project solution data.
*/

const createSolution = function ( bodyData,token ) {
     return new Promise(async (resolve, reject) => {
         try {
             
             const url = ML_CORE_URL + CONSTANTS.endpoints.CREATE_IMPROVEMENT_PROJECT_SOLUTION;
            
             const options = {
                 headers : {
                     "content-type": "application/json",
                     "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN,
                     "x-authenticated-user-token" : token
                 },
                 json : bodyData
             };
 
             request.post(url,options,kendraCallback);
 
             function kendraCallback(err, data) {
 
                 let result = {
                     success : true
                 };
 
                 if (err) {
                     result.success = false;
                 } else {
                    
                     let response = data.body;
 
                     if( response.status === HTTP_STATUS_CODE['ok'].status ) {
                         result["data"] = response.result;
                     } else {
                         result.success = false;
                     }
                 }
 
                 return resolve(result);
             }
 
         } catch (error) {
             return reject(error);
         }
     })
 }

/**
  * List of solutions based on role and location.
  * @function
  * @name solutionBasedOnRoleAndLocation
  * @param {String} token - User token.
  * @param {Object} bodyData - Requested body data.
  * @param {String} searchText - Text to search.
  * @returns {JSON} - List of user targetted solutions.
*/

const solutionBasedOnRoleAndLocation = function ( token,bodyData,typeAndSubType,searchText = "" ) {
    return new Promise(async (resolve, reject) => {
        try {
            
            let url = 
            ML_CORE_URL + CONSTANTS.endpoints.SOLUTION_BASED_ON_ROLE_LOCATION+ "?type=" + typeAndSubType + "&subType=" + typeAndSubType;

            if( searchText !== "" ) {
                url = url + "&search=" + searchText;
            }

            const options = {
                headers : {
                    "content-type": "application/json",
                    "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN,
                    "x-authenticated-user-token" : token
                },
                json : bodyData
            };

            request.post(url,options,kendraCallback);

            function kendraCallback(err, data) {

                let result = {
                    success : true
                };

                if (err) {
                    result.success = false;
                } else {
                    
                    let response = data.body;
                    
                    if( response.status === HTTP_STATUS_CODE['ok'].status ) {
                        result["data"] = response.result;
                    } else {
                        result.success = false;
                    }
                }

                return resolve(result);
            }

        } catch (error) {
            return reject(error);
        }
    })
}

/**
  * Solution details based on role and location.
  * @function
  * @name solutionDetailsBasedOnRoleAndLocation
  * @param {String} token - User token.
  * @param {Object} bodyData - Requested body data.
  * @param {String} solutionId - Targeted solution id.
  * @returns {JSON} - List of user targetted solutions.
*/

const solutionDetailsBasedOnRoleAndLocation = function ( token,bodyData,solutionId ) {
    return new Promise(async (resolve, reject) => {
        try {
            
            const url = 
            ML_CORE_URL + CONSTANTS.endpoints.SOLUTION_DETAILS_BASED_ON_ROLE_LOCATION + "/" + solutionId;

            const options = {
                headers : {
                    "content-type": "application/json",
                    "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN,
                    "x-authenticated-user-token" : token
                },
                json : bodyData
            };

            request.post(url,options,kendraCallback);

            function kendraCallback(err, data) {

                let result = {
                    success : true
                };

                if (err) {
                    result.success = false;
                } else {
                    
                    let response = data.body;
                    
                    if( response.status === HTTP_STATUS_CODE['ok'].status ) {
                        result["data"] = response.result;
                    } else {
                        result.success = false;
                    }
                }

                return resolve(result);
            }

        } catch (error) {
            return reject(error);
        }
    })
}

/**
  * program Join Api.
  * @function
  * @name joinProgram
  * @param {String} userToken - User token.
  * @param {Object} bodyData - Requested body data.
  * @param {String} programId - program id.
  * @returns {JSON} - Program Join Status.
*/
const joinProgram = function (programId,bodyData,userToken) {
    return new Promise(async (resolve, reject) => {
        try {
            
            const url = 
            ML_CORE_URL + CONSTANTS.endpoints.PROGRAM_JOIN + "/" + programId;
            const options = {
                headers : {
                    "content-type": "application/json",
                    "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN,
                    "x-authenticated-user-token" : userToken,
                },
               
            };

            if ( bodyData.appVersion !== "" ) {
                options.headers.appversion = bodyData.appVersion;
                delete bodyData.appVersion
            }

            if ( bodyData.appName !== "" ) {
                options.headers.appname = bodyData.appName;
                delete bodyData.appName
            } 


            options.json = bodyData
            request.post(url,options,kendraCallback);

            function kendraCallback(err, data) {

                let result = {
                    success : true
                };

                if (err) {
                    result.success = false;
                } else {
                    
                    let response = data.body;
                    if( response.status === HTTP_STATUS_CODE['ok'].status ) {
                        result["data"] = response.result;
                    } else {
                        result.success = false;
                    }
                }

                return resolve(result);
            }

        } catch (error) {
            return reject(error);
        }
    })
}


const checkIfSolutionIsTargetedForUserProfile = function ( token,bodyData,solutionId ) {
    return new Promise(async (resolve, reject) => {
        try {
            
            const url = 
            ML_CORE_URL + CONSTANTS.endpoints.IS_TARGETED_BASED_ON_USER_PROFILE + "/" + solutionId;

            const options = {
                headers : {
                    "content-type": "application/json",
                    "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN,
                    "x-authenticated-user-token" : token
                },
                json : bodyData
            };

            request.post(url,options,verifyTargetedSolutionCallback);

            function verifyTargetedSolutionCallback(err, data) {

                let result = {
                    success : true
                };

                if (err) {
                    result.success = false;
                } else {
                    
                    let response = data.body;
                    if( response.status === HTTP_STATUS_CODE['ok'].status ) {
                        result["data"] = response.result;
                    } else {
                        result.success = false;
                    }
                }

                return resolve(result);
            }

        } catch (error) {
            return reject(error);
        }
    })
}
module.exports = {
    entityTypesDocuments : entityTypesDocuments,
    rolesDocuments : rolesDocuments,
    formDetails : formDetails,
    entityDocuments: entityDocuments,
    createUserProgramAndSolution : createUserProgramAndSolution,
    getProfile : getProfile,
    updateUserProfile : updateUserProfile,
    userPrivatePrograms : userPrivatePrograms,
    getUsersByEntityAndRole : getUsersByEntityAndRole,
    createSolution: createSolution,
    solutionBasedOnRoleAndLocation : solutionBasedOnRoleAndLocation,
    solutionDetailsBasedOnRoleAndLocation : solutionDetailsBasedOnRoleAndLocation,
    getDownloadableUrl : getDownloadableUrl, 
    joinProgram: joinProgram,
    checkIfSolutionIsTargetedForUserProfile:checkIfSolutionIsTargetedForUserProfile
};

