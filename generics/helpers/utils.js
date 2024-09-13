/**
 * name : utils.js
 * author : Aman Karki
 * Date : 13-July-2020
 * Description : All utility functions.
*/
// Dependencies
const {validate : uuidValidate,v4 : uuidV4} = require('uuid');
const packageData = require(PROJECT_ROOT_DIRECTORY + "/package.json");
/**
  * convert camel case to title case.
  * @function
  * @name camelCaseToTitleCase
  * @param {String} in_camelCaseString - String of camel case.
  * @returns {String} returns a titleCase string. ex: helloThereMister, o/p: Hello There Mister
*/

function camelCaseToTitleCase(in_camelCaseString) {
  var result = in_camelCaseString // "ToGetYourGEDInTimeASongAboutThe26ABCsIsOfTheEssenceButAPersonalIDCardForUser456InRoom26AContainingABC26TimesIsNotAsEasyAs123ForC3POOrR2D2Or2R2D"
    .replace(/([a-z])([A-Z][a-z])/g, "$1 $2") // "To Get YourGEDIn TimeASong About The26ABCs IsOf The Essence ButAPersonalIDCard For User456In Room26AContainingABC26Times IsNot AsEasy As123ForC3POOrR2D2Or2R2D"
    .replace(/([A-Z][a-z])([A-Z])/g, "$1 $2") // "To Get YourGEDIn TimeASong About The26ABCs Is Of The Essence ButAPersonalIDCard For User456In Room26AContainingABC26Times Is Not As Easy As123ForC3POOr R2D2Or2R2D"
    .replace(/([a-z])([A-Z]+[a-z])/g, "$1 $2") // "To Get Your GEDIn Time ASong About The26ABCs Is Of The Essence But APersonal IDCard For User456In Room26AContainingABC26Times Is Not As Easy As123ForC3POOr R2D2Or2R2D"
    .replace(/([A-Z]+)([A-Z][a-z][a-z])/g, "$1 $2") // "To Get Your GEDIn Time A Song About The26ABCs Is Of The Essence But A Personal ID Card For User456In Room26A ContainingABC26Times Is Not As Easy As123ForC3POOr R2D2Or2R2D"
    .replace(/([a-z]+)([A-Z0-9]+)/g, "$1 $2") // "To Get Your GEDIn Time A Song About The 26ABCs Is Of The Essence But A Personal ID Card For User 456In Room 26A Containing ABC26Times Is Not As Easy As 123For C3POOr R2D2Or 2R2D"

    // Note: the next regex includes a special case to exclude plurals of acronyms, e.g. "ABCs"
    .replace(/([A-Z]+)([A-Z][a-rt-z][a-z]*)/g, "$1 $2") // "To Get Your GED In Time A Song About The 26ABCs Is Of The Essence But A Personal ID Card For User 456In Room 26A Containing ABC26Times Is Not As Easy As 123For C3PO Or R2D2Or 2R2D"
    .replace(/([0-9])([A-Z][a-z]+)/g, "$1 $2") // "To Get Your GED In Time A Song About The 26ABCs Is Of The Essence But A Personal ID Card For User 456In Room 26A Containing ABC 26Times Is Not As Easy As 123For C3PO Or R2D2Or 2R2D"

    // Note: the next two regexes use {2,} instead of + to add space on phrases like Room26A and 26ABCs but not on phrases like R2D2 and C3PO"
    .replace(/([A-Z]{2,})([0-9]{2,})/g, "$1 $2") // "To Get Your GED In Time A Song About The 26ABCs Is Of The Essence But A Personal ID Card For User 456 In Room 26A Containing ABC 26 Times Is Not As Easy As 123 For C3PO Or R2D2 Or 2R2D"
    .replace(/([0-9]{2,})([A-Z]{2,})/g, "$1 $2") // "To Get Your GED In Time A Song About The 26 ABCs Is Of The Essence But A Personal ID Card For User 456 In Room 26A Containing ABC 26 Times Is Not As Easy As 123 For C3PO Or R2D2 Or 2R2D"
    .trim();

  // capitalize the first letter
  return result.charAt(0).toUpperCase() + result.slice(1);
}

 /**
  * Convert hyphen case string to camelCase.
  * @function
  * @name hyphenCaseToCamelCase
  * @param {String} string - String in hyphen case.
  * @returns {String} returns a camelCase string.
*/

function hyphenCaseToCamelCase(string) {
  return string.replace(/-([a-z])/g, function (g) { return g[1].toUpperCase(); });
}

 /**
  * convert string to lowerCase.
  * @function
  * @name lowerCase
  * @param {String} str 
  * @returns {String} returns a lowercase string. ex: HELLO, o/p: hello
*/

function lowerCase(str) {
  return str.toLowerCase()
}

 /**
  * check whether the given string is url.
  * @function
  * @name checkIfStringIsUrl - check whether string is url or not.
  * @param {String} str 
  * @returns {Boolean} returns a Boolean value. ex:"http://example.com:3000/pathname/?search=test" , o/p:true
*/

function checkIfStringIsUrl(str) {
  var pattern = new RegExp('^(https?:\\/\\/)?' + // protocol
    '((([a-z\\d]([a-z\\d-]*[a-z\\d])*)\\.?)+[a-z]{2,}|' + // domain name
    '((\\d{1,3}\\.){3}\\d{1,3}))' + // OR ip (v4) address
    '(\\:\\d+)?(\\/[-a-z\\d%_.~+]*)*' + // port and path
    '(\\?[;&a-z\\d%_.~+=-]*)?' + // query string
    '(\\#[-a-z\\d_]*)?$', 'i'); // fragment locator
  return pattern.test(str);
}

 /**
  * Parse a single column.
  * @function
  * @name valueParser - Parse value
  * @param {String} dataToBeParsed - data to be parsed. 
  * @returns {Object} returns parsed data
*/

function valueParser(dataToBeParsed) {

  let parsedData = {}

  Object.keys(dataToBeParsed).forEach(eachDataToBeParsed => {
    parsedData[eachDataToBeParsed] = dataToBeParsed[eachDataToBeParsed].trim()
  })

  if(parsedData._arrayFields && parsedData._arrayFields.split(",").length > 0) {
    parsedData._arrayFields.split(",").forEach(arrayTypeField => {
      if (parsedData[arrayTypeField]) {
        parsedData[arrayTypeField] = parsedData[arrayTypeField].split(",")
      }
    })
  }

  return parsedData
}

/**
     * Convert string to boolean.
     * @method
     * @name convertStringToBoolean
     * @param {String} stringData -String data.         
     * @returns {Boolean} - Boolean data.  
   */
  
  function convertStringToBoolean(stringData) {
    let stringToBoolean = (stringData === "TRUE" || stringData === "true" || stringData === true );
    return stringToBoolean;
  }

    /**
   * List of boolean data from a given model.
   * @method
   * @name getAllBooleanDataFromModels  
   * @param schema - schema    
   * @returns {Array} Boolean data.
   */

  function getAllBooleanDataFromModels(schema) {

    let defaultSchema = Object.keys(schema);

    let booleanValues = [];

    defaultSchema.forEach(singleSchemaKey=>{

      let currentSchema = schema[singleSchemaKey];

      if( 
        currentSchema.hasOwnProperty('default') && 
        typeof currentSchema.default === "boolean" 
      ) {
        booleanValues.push(singleSchemaKey);
      }
    });

    return booleanValues;
  }

   /**
    * check whether id is mongodbId or not.
    * @function
    * @name isValidMongoId
    * @param {String} id 
    * @returns {Boolean} returns whether id is valid mongodb id or not.  
  */

  function isValidMongoId(id) {
    return ObjectId.isValid(id) && new ObjectId(id).toString() === id;
  }

  /**
  * Get epoch time from current date.
  * @function
  * @name epochTime
  * @returns {Date} returns epoch time.  
  */

function epochTime() {
  var currentDate = new Date();
  currentDate = currentDate.getTime();
  return currentDate;
}

/**
  * Convert Project Status
  * @function
  * @name convertProjectStatus
  * @returns {String} returns converted project status
  */

function convertProjectStatus(status) {

    let convertedStatus;
 
    if ( status == CONSTANTS.common.NOT_STARTED_STATUS ) {
        convertedStatus = CONSTANTS.common.STARTED;
    } else if ( status == CONSTANTS.common.COMPLETED_STATUS ) {
        convertedStatus = CONSTANTS.common.SUBMITTED_STATUS;
    } else {
        convertedStatus = status;
    } 
    
    return convertedStatus;
}

/**
  * Revert Project Status For Older App
  * @function
  * @name revertProjectStatus
  * @returns {String} returns reverted project status
  */

 function revertProjectStatus(status) {

  let revertedStatus;

  if ( status == CONSTANTS.common.STARTED ) {
      revertedStatus = CONSTANTS.common.NOT_STARTED_STATUS;
  } else if ( status == CONSTANTS.common.SUBMITTED_STATUS ) {
      revertedStatus = CONSTANTS.common.COMPLETED_STATUS;
  } else {
      revertedStatus = status;
  } 

  return revertedStatus;
}

/**
   * revert status or not
   * @method
   * @name revertStatusorNot 
   * @param {String} appVersion - app Version.
   * @returns {Boolean} - true or false
*/

function revertStatusorNot( appVersion ) {

  let versions = ["4.10", "4.11", "4.12" ];

  let appVer = appVersion.split('.',2).join('.');
  if ( versions.includes(appVer)) {
      return false
  } else {

      let appVersionNo = Number(appVer);
      if ( !isNaN(appVersionNo) && appVersionNo < 4.7 ) {
          return true
      } else {
          return false
      }
  }

}

/**
  * check whether string is valid uuid.
  * @function
  * @name checkValidUUID
  * @param {String} uuids 
  * @returns {Boolean} returns a Boolean value true/false
*/

function checkValidUUID(uuids) {

  var validateUUID = true;
  if(Array.isArray(uuids)){
      for (var i = 0; uuids.length > i; i++) {
          if(!uuidValidate(uuids[i])){
            validateUUID = false
          }
      }
  }else {
     validateUUID = uuidValidate(uuids);
  }
  return validateUUID;
}

/**
  * make dates comparable
  * @function
  * @name createComparableDates
  * @param {String} dateArg1
  * @param {String} dateArg2
  * @returns {Object} - date object
*/

function createComparableDates(dateArg1, dateArg2) {
  let date1
  if(typeof dateArg1 === "string") {
    date1 = new Date(dateArg1.replace( /(\d{2})-(\d{2})-(\d{4})/, "$2/$1/$3"))
  } else {
    date1 = new Date(dateArg1)
  }
  
  let date2
  if(typeof dateArg2 === "string") {
    date2 = new Date(dateArg2.replace( /(\d{2})-(\d{2})-(\d{4})/, "$2/$1/$3"))
  } else {
    date2 = new Date(dateArg2)
  }

  date1.setHours(0)
  date1.setMinutes(0)
  date1.setSeconds(0)
  date2.setHours(0)
  date2.setMinutes(0)
  date2.setSeconds(0)
  return({
    dateOne: date1,
    dateTwo: date2
  })
}

/**
  * count attachments
  * @function
  * @name noOfElementsInArray
  * @param {Object} data - data to count
  * @param {Object} filter -  filter data
  * @returns {Number} - attachment count
*/

function noOfElementsInArray(data, filter = {}) {
  if ( !filter || !(Object.keys(filter).length > 0) ) {
    return data.length;
  }
  if ( !(data.length > 0) ) {
    return 0;
  } else {
    if ( filter.value == "all" ){
      return data.length;
    } else {
      let count = 0;
      for ( let attachment = 0; attachment < data.length; attachment++ ) {
        if ( data[attachment][filter.key] == filter.value ) {
          count++
        }
      }
      return count;
    }
  }
}

/**
  * validate lhs and rhs using operator passed as String/ Number
  * @function
  * @name operatorValidation
  * @param {Number or String} valueLhs 
  * @param {Number or String} valueRhs 
  * @returns {Boolean} - validation result
*/

function operatorValidation(valueLhs, valueRhs, operator) {
  return new Promise(async (resolve, reject) => { 
      let result = false;
      if (operator == "==" ) {
          result = (valueLhs == valueRhs) ? true : false
      } else if (operator == "!=" ) {
          result = (valueLhs != valueRhs) ? true : false
      } else if (operator == ">" ) {
          result = (valueLhs > valueRhs) ? true : false
      } else if (operator == "<" ) {
          result = (valueLhs < valueRhs) ? true : false
      } else if (operator == "<=" ) {
        result = (valueLhs <= valueRhs) ? true : false
      } else if (operator == ">=" ) {
        result = (valueLhs >= valueRhs) ? true : false
      } 
      return resolve(result)
  })          
}

/**
 * generate uuid
 * @function
 * @name generateUUId
 * @returns {String} returns uuid.
 */

function generateUUId() {
  return uuidV4();
}


/**
 * generate skeleton telemetry raw event
 * @function
 * @name generateTelemetryEventSkeletonStructure
 * @returns {Object} returns uuid.
 */
 function generateTelemetryEventSkeletonStructure() {
  let telemetrySkeleton = {
    eid: "",
    ets: epochTime(),
    ver: CONSTANTS.common.TELEMETRY_VERSION,
    mid: generateUUId(),
    actor: {},
    context: {
      channel: "",
      pdata: {
        id: process.env.ID,
        ver: packageData.version,
      },
      env: "",
      cdata: [],
      rollup: {},
    },
    object: {},
    edata: {},
  };
  return telemetrySkeleton;
}

/**
 * generate telemetry event
 * @function
 * @name generateTelemetryEvent
 * @returns {Object} returns uuid.
 */
 function generateTelemetryEvent(rawEvent) {
  let telemetryEvent = {
    timestamp: new Date(),
    msg: JSON.stringify(rawEvent),
    lname: "",
    tname: "",
    level: "",
    HOSTNAME: "",
    "application.home": "",
  };
  return telemetryEvent;
}


/**
  * check the uuid is valid
  * @function
  * @name checkIfValidUUID
  * @returns {String} returns boolean.  
*/

function checkIfValidUUID(value) {
  const regexExp = /^[0-9a-fA-F]{8}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{4}\b-[0-9a-fA-F]{12}$/gi;
  return regexExp.test(value);
}

/**
  * filter out location id and code
  * @function
  * @name filterLocationIdandCode
  * @returns {Object} - Object contain locationid and location code array.  
*/

function filterLocationIdandCode(dataArray) {
  let locationIds = [];
  let locationCodes = [];
  dataArray.forEach(element=>{
      if (this.checkIfValidUUID(element)) {
          locationIds.push(element);
      } else {
          locationCodes.push(element);
      }
  });
  return ({
    ids : locationIds,
    codes : locationCodes
  });
}



module.exports = {
  camelCaseToTitleCase : camelCaseToTitleCase,
  lowerCase : lowerCase,
  checkIfStringIsUrl : checkIfStringIsUrl,
  hyphenCaseToCamelCase : hyphenCaseToCamelCase,
  valueParser : valueParser,
  convertStringToBoolean : convertStringToBoolean,
  getAllBooleanDataFromModels : getAllBooleanDataFromModels,
  epochTime : epochTime,
  isValidMongoId : isValidMongoId,
  convertProjectStatus : convertProjectStatus,
  revertProjectStatus:revertProjectStatus,
  revertStatusorNot:revertStatusorNot,
  checkValidUUID : checkValidUUID,
  createComparableDates : createComparableDates,
  noOfElementsInArray : noOfElementsInArray,
  operatorValidation : operatorValidation,
  generateTelemetryEventSkeletonStructure : generateTelemetryEventSkeletonStructure,
  generateTelemetryEvent : generateTelemetryEvent,
  filterLocationIdandCode : filterLocationIdandCode,
  checkIfValidUUID : checkIfValidUUID
};
