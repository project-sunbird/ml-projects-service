/**
 * name : certificate.js
 * author : Vishnu
 * Date : 07-Oct-2022
 * Description : Sunbird-RC certificate api.
 */

//dependencies
const request = require('request');

/**
  * Project certificate creation
  * @function
  * @name createCertificate
  * @param {Object} bodyData - Body data
  * @returns {JSON} - Certificate creation details.
*/

const createCertificate = function (bodyData) {
    return new Promise(async (resolve, reject) => {
        try {
            
            const ML_PROJECT_URL = `http://${process.env.SERVICE_NAME}:${process.env.APPLICATION_PORT}`;
            const callbackUrl = ML_PROJECT_URL + CONSTANTS.endpoints.PROJECT_CERTIFICATE_API_CALLBACK;
            let certificateCreateUrl =  
            process.env.CERTIFICATE_SERVICE_URL + 
            CONSTANTS.endpoints.CERTIFICATE_CREATE + "?mode=async&callback=" + callbackUrl;
            const options = {
                headers : {
                    "content-type": "application/json"
                },
                json : bodyData
            };

            console.log("bodyData : ",bodyData)
            console.log("certificateCreateUrl : ",certificateCreateUrl)

            request.post(certificateCreateUrl,options,certificateCallback);

            function certificateCallback(err, data) {
                console.log("line 39 raw data from RC call :",JSON.stringify(data));
                let result = {
                    success : true
                };
                if (err) {
                    result.success = false;
                    console.log("line 45 error from RC call error :",err.message);
                } else {
                    let response = data.body;
                    console.log("certificate success response: ",JSON.stringify(response))
                    if( response.params && response.params.status && response.params.status === "SUCCESSFUL" ) {
                        result["data"] = response.result;
                    } else {
                        result.success = false;
                    }
                }
                return resolve(result);
            }

        } catch (error) {
            console.log("line 58 catch block : ",error.message)
            return reject(error);
        }
    })
}

/**
  * Project certificate issuer-kid
  * @function
  * @name getCertificateIssuerKid
  * @returns {JSON} - Certificate issuer kid details.
*/

const getCertificateIssuerKid = function () {
    return new Promise(async (resolve, reject) => {
        try {
            let issuerKidUrl =  
            process.env.CERTIFICATE_SERVICE_URL + CONSTANTS.endpoints.GET_CERTIFICATE_KID;
            
            let bodyData = {"filters": {}};
            
            const options = {
                headers : {
                    "Content-Type": "application/json"
                },
                json : bodyData
            }; 
            console.log("issuer Kid url : ",issuerKidUrl);
            console.log("issuer Kid bodyData : ",JSON.stringify(bodyData));
            request.post(issuerKidUrl,options,getKidCallback);
            function getKidCallback(err, data) {
                let result = {
                    success : true
                };
                
                if (err) {
                    console.log("KID rc call error : ",err.message)
                    result.success = false;
                } else {
                    let response = data.body;
                    console.log("KID success response : ",JSON.stringify(response))
                    if( response.length >  0 && response[0].osid && response[0].osid !== "" ) {
                        result["data"] = response[0].osid;
                    } else {
                        result.success = false;
                    }
                }
                return resolve(result);
            }
            setTimeout(function () {
                return resolve (result = {
                    success : false
                 });
            }, CONSTANTS.common.SERVER_TIME_OUT);

        } catch (error) {
            console.log("catch error : ",error.message)
            return reject(error);
        }
    })
}

module.exports = {
    createCertificate : createCertificate,
    getCertificateIssuerKid : getCertificateIssuerKid
}