/**
 * name : helper.js
 * author : vishnu
 * created-date : 26-Oct-2022
 * Description : certificate validation helper functionality.
 */

// Dependencies

/**
    * certificateValidationsHelper
    * @class
*/

module.exports = class certificateValidationsHelper {

    /**
     * validate certificate criteria.
     * @method
     * @name criteriaValidation 
     * @param {Object} data - project data for certificate creation 
     * @returns 
    */

    static criteriaValidation(data) {
        return new Promise(async (resolve, reject) => {
            try {
                let criteria = data.certificate.criteria; // criteria conditions for certificate
                let validationResult = [];
                let validationMessage = "";
                let validationExpression = criteria.expression
                if ( criteria.conditions &&  Object.keys(criteria.conditions).length > 0 ) {
                    let conditions = criteria.conditions;
                    let conditionKeys = Object.keys(conditions)

                    for ( let index = 0; index < conditionKeys.length; index++ ) {
                        // correntCondition contain the prefinal level data
                        let currentCondition = conditions[conditionKeys[index]];

                        //now pass expression and validation scope to another function which will start the validation procedure
                        let validation = await _subCriteriaValidation( currentCondition.conditions, currentCondition.expression, data );
                        
                        validationResult.push(validation.success);
                        ( validation.success == false ) ? validationMessage = validationMessage + " " + currentCondition.validationText : "";
                    }
                    // validate criteria using defined expression 
                    let criteriaValidation = await _criteriaExpressionValidation( validationExpression, conditionKeys, validationResult )
                    return resolve({
                        success: criteriaValidation,
                        message: ( criteriaValidation == false ) ? validationMessage : CONSTANTS.common.PROJECT_CERTIFICATE_GENERATED_SUCCESSFULLY
                    });
                }
                return resolve({
                    success: false
                })
            } catch (error) {
                return resolve({
                    success: false,
                    message: error.message,
                    data: {}
                });
            }
        })
    }
};

/**
 * _subCriteriaValidation.
 * @method
 * @name _subCriteriaValidation 
 * @param {Object} conditions - condition data.
 * @param {String} expression - validation expression
 * @returns {Boolean} validation.
*/

function _subCriteriaValidation(conditions, expression, data) {
   return new Promise(async (resolve, reject) => {
        try {
            let conditionKeys = Object.keys(conditions)
            let validationResult = [];
           // loop throug conditions of subcriterias
            for ( let index = 0; index < conditionKeys.length; index++ ) {
                let currentCondition = conditions[conditionKeys[index]];
                // correntCondition contain the prefinal level data
                //now pass expression and validation scope to another function which will start the validation procedure
                let validation = await _validateCriteriaConditions( currentCondition, data );
                validationResult.push(validation);
            }
            // validate expression 
            let subcriteriaValidation = await _criteriaExpressionValidation( expression, conditionKeys, validationResult )
            return resolve({
                success: subcriteriaValidation
            });

        } catch (error) {
            return resolve({
                message: error.message,
                success: false,
                status:
                    error.status ?
                        error.status : HTTP_STATUS_CODE['internal_server_error'].status
            })
        }
    })
}

/**
 * _validateCriteriaConditions.
 * @method
 * @name _validateCriteriaConditions 
 * @param {Object} condition - condition data.
 * @param {String} data - validation data
 * @returns {Boolean} validation.
*/

function _validateCriteriaConditions(condition, data) {
    return new Promise(async (resolve, reject) => {
        try {
            let result = false;
            if ( !condition.function || condition.function == "" ) { 
                if ( condition.scope == CONSTANTS.common.PROJECT ){
                    // if validation is on completedDate
                    if ( condition.key == "completedDate") {
                        let comparableDates = UTILS.createComparableDates( data[condition.key], condition.value );
                        data[condition.key] = comparableDates.dateOne;
                        condition.value = comparableDates.dateTwo;
                    }
                    // validate prject value with condition value
                    result = UTILS.operatorValidation( data[condition.key], condition.value, condition.operator );
                    
                } 
            } else {
                try {
                    let valueFromProject = 0;
                    // if: condition is in scope of project and contains a function to check
                    if ( condition.scope == CONSTANTS.common.PROJECT ) {
                        // get count of attachments at project level
                        valueFromProject = UTILS.noOfElementsInArray( data[condition.key], condition.filter ); 
                    } else if ( condition.scope == CONSTANTS.common.TASK_ATTACHMENT ){
                        // for task attachment validatiion _id of specific task or "all" key should be passed in an array called taskDetails
                        let tasksAttachments = [];
                        let projectTasks = data.tasks;
                        // check tasks and taskDetails exists
                        if ( projectTasks && projectTasks.length > 0 && condition.taskDetails.length > 0 &&  condition.taskDetails[0] == "all" ) {
                            // loop through tasks to get attachments
                            for ( let tasksIndex = 0; tasksIndex < projectTasks.length; tasksIndex++ ) {
                                
                                if ( projectTasks[tasksIndex][condition.key] && projectTasks[tasksIndex][condition.key].length > 0 ) 
                                {
                                    tasksAttachments.push(...projectTasks[tasksIndex][condition.key])
                                }
                            }

                        } else if ( projectTasks && projectTasks.length > 0 && condition.taskDetails.length > 0 ) {
                            
                            // specific task Id( from projectTemplates ) or Ids are passed for attachment validation
                            for ( let tasksIndex = 0; tasksIndex < projectTasks.length; tasksIndex++  ) {
                                for ( let taskDetailsPointer = 0; taskDetailsPointer < condition.taskDetails.length; taskDetailsPointer++ ) {
                                    // get attachments data of specified task/ tasks
                                    if ( projectTasks[tasksIndex].referenceId == condition.taskDetails[taskDetailsPointer] && projectTasks[tasksIndex][condition.key] && projectTasks[tasksIndex][condition.key].length > 0 ) {
                                        tasksAttachments.push(...projectTasks[tasksIndex][condition.key])
                                    }
                                }
                                
                            }

                        } else {
                            return resolve(result)
                        }
                        if ( !(tasksAttachments.length > 0) ) {
                            return resolve(result)
                        }
                        // get task attachments count
                        valueFromProject = UTILS.noOfElementsInArray( tasksAttachments, condition.filter ); 
                    }
                    // validate against condition value
                    result =  UTILS.operatorValidation( valueFromProject, condition.value, condition.operator );

                } catch (fnError) {
                    return resolve(result)
                }
            }            
            return resolve(result);
        } catch (error) {
            return resolve({
                message: error.message,
                success: false,
                status:
                    error.status ?
                        error.status : HTTP_STATUS_CODE['internal_server_error'].status
            })
        }
    })
}
/**
 * _criteriaExpressionValidation
 * @method
 * @name _criteriaExpressionValidation 
 * @param {String} expression - criteria expression
 * @param {Array} keys - condition keys
 * @param {Array} result - condition result
 * @returns {Boolean} validation result.
*/

function _criteriaExpressionValidation(expression, keys, result) {
    return new Promise(async (resolve, reject) => {
        try {
            
            if( expression == "" ||
                !(keys.length > 0) ||
                !(result.length > 0) ||
                keys.length != result.length ) {
                return resolve(false);
            }
            // generate expression string that can be evaluated
            for ( let pointerToKeys = 0; pointerToKeys < keys.length; pointerToKeys++ ) {
                expression = expression.replace(keys[pointerToKeys],result[pointerToKeys].toString())
            }
            let evalResult = eval(expression)
            
            return resolve(evalResult);

        } catch (error) {
            return resolve(false);
        }
    })
}






