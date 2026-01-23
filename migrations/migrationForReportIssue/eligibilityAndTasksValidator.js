const fs = require("fs")
const path = require('path');
const UTILS = require("../../generics/helpers/utils");
const request = require('request');
const _ = require('lodash');


require('dotenv').config({
  path: path.resolve(__dirname, '../../.env'),
});

const output_dir = path.join(__dirname, "output");
if (!fs.existsSync(output_dir)) {
  fs.mkdirSync(output_dir, { recursive: true });
};


function updateTasksUsingPublicProject(projects, publicProject) {
  if (
    !publicProject ||
    !Array.isArray(publicProject.tasks) ||
    publicProject.tasks.length === 0
  ) {
    return projects;
  }
  const referenceTasks = publicProject.tasks;

  for (let project of projects) {
    if (!project || !Array.isArray(project.tasks)) continue;
    
    // Loop through reference project tasks
    for (const refTask of referenceTasks) {
      if (!refTask || !refTask.externalId || !refTask.referenceId) continue;
      
      const lastDashIndex = refTask.externalId.lastIndexOf('-');
      if (lastDashIndex === -1) continue;
      
      const prefix = refTask.externalId.substring(0, lastDashIndex);
      
      // Loop through current project tasks
      for (const task of project.tasks) {
        if (!task || !task.externalId) continue;

        if (task.externalId.startsWith(prefix)) {
          task.referenceId = refTask.referenceId;
          task.externalId = refTask.externalId;
          break; // stop after first match
        }
      }
    }
  }
  return projects;
}

async function criteriaValidation(data) {
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
                    success: criteriaValidation
                });
            }
            return resolve({
                success: false
            })
        } catch (error) {
            return reject({
                success: false,
                message: error.message,
                data: {}
            });
        }
    })
}

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
            return reject({
                message: error.message,
                success: false
            })
        }
    })
}

 function _validateCriteriaConditions(condition, data) {
    return new Promise(async (resolve, reject) => {
        try {
            let result = false;
            if ( !condition.function || condition.function == "" ) { 
                if ( condition.scope == "project" ){
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
                    if ( condition.scope == "project" ) {
                        // get count of attachments at project level
                        valueFromProject = UTILS.noOfElementsInArray( data[condition.key], condition.filter ); 
                    } else if ( condition.scope == "task" ){
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
                        if ( !tasksAttachments.length > 0 ) {
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
            return reject({
                message: error.message,
                success: false
            })
        }
    })
}

function _criteriaExpressionValidation(expression, keys, result) {
    return new Promise(async (resolve, reject) => {
        try {
            
            if( expression == "" ||
                !keys.length > 0 ||
                !result.length > 0 ||
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
            return reject(false);
        }
    })
}

async function updateCorruptedProjectsInDB(projects, DB, solution, program, doUpdate = false, masterFilePath) {

  const projectsCollection = await DB.collection('projects');
  const bulkOps = [];
  const eligibleIds = [];
  const nonEligibleIds = [];

  for (const project of projects) {
    // if (!project || !project._id || !Array.isArray(project.tasks)) continue;

    const projectId = project._id.toString();

    if (project.eligible === true) {
      eligibleIds.push(projectId);

      bulkOps.push({
        projectId, // 👈 important for failure mapping
        updateOne: {
          filter: { _id: project._id },
          update: {
            $set: {
                isAPrivateProgram: false,
                isMigratedDueToReportIssue: true,
                programId: program._id,
                programExternalId: program.externalId,
                solutionId: solution._id,
                solutionExternalId: solution.externalId,
                programInformation: {
                    _id: program._id,
                    externalId: program.externalId,
                    name: program.name,
                    description: program.description,
                    isAPrivateProgram: false,
                },
                solutionInformation: {
                    _id: solution._id,
                    externalId: solution.externalId,
                    name: solution.name,
                    description: solution.description,
                    isAPrivateProgram: false,
                },
                tasks: project.tasks,
                isMigratedDueToReportIssue: true,
                "certificate.eligible": true
            }
          }
        }
      });
    } else {
      nonEligibleIds.push(projectId);
    }
  }

  let jsonData = {};
  if(fs.existsSync(masterFilePath)){
    const fileContent = fs.readFileSync(masterFilePath,"utf8");
    jsonData = fileContent ? JSON.parse(fileContent) : {};
  }

  if (Array.isArray(jsonData["certificateEligibleProjectIds"])) {
    jsonData["certificateEligibleProjectIds"].push(...eligibleIds);
  } else {
    jsonData["certificateEligibleProjectIds"] = [...eligibleIds];
  }

  if (Array.isArray(jsonData["certificateNonEligibleProjectIds"])) {
    jsonData["certificateNonEligibleProjectIds"].push(...nonEligibleIds);
  } else {
    jsonData["certificateNonEligibleProjectIds"] = [...nonEligibleIds];
  }
  
  fs.writeFileSync(
    masterFilePath,
    JSON.stringify(jsonData, null, 2),
    'utf8'
  );

  if (!bulkOps.length) {
    console.log('No eligible projects found to update');
    return;
  }

  /* Perform updates ONLY if --update=true flag is passed */
  if (!doUpdate) {
    console.log("Dry run only. Skipping DB updates.");
    return;
  }

  try {
    const result = await projectsCollection.bulkWrite(
      bulkOps.map(({ projectId, ...op }) => op),
      { ordered: false }
    );

  } catch (error) {
    console.error('Bulk update partially failed');
  }
}

function requestPromise(options) {
  return new Promise((resolve, reject) => {
    request(options, (error, response, body) => {
      if (error) {
        return reject({
          message: error.message,
          error
        });
      }
      return resolve(body);
    });
  });
}  
  
async function reIssueCertificates(projects, userToken, masterFilePath) {

  if (!Array.isArray(projects) || projects.length === 0) {
    console.log("No projects provided for certificate reissue");
    return;
  }

  const apiResponses = {};

  for (const project of projects) {
    if (project.eligible !== true) continue;

    const projectId = project._id.toString();

    try {
      const responseBody = await requestPromise({
        method: 'POST',
        url: `http://localhost:${process.env.APPLICATION_PORT}/v1/userProjects/certificateReIssue/${projectId}`,
        headers: {
          "content-type": "application/json",
          "internal-access-token": process.env.INTERNAL_ACCESS_TOKEN,
          "x-authenticated-user-token": userToken
        }
      });

      apiResponses[projectId] =
      responseBody !== undefined && responseBody !== null
        ? responseBody
        : null;
    

      console.log(`✅ API success for project ${projectId}`);
    } catch (error) {
      apiResponses[projectId] = {
        success: false,
        statusCode: error.statusCode || 500,
        error: error.body || error.message || 'Unknown error'
      };

      console.error(`❌ API failed for project ${projectId}`);
    }
  }

  let jsonData = {};
  if (fs.existsSync(masterFilePath)) {
    const fileContent = fs.readFileSync(masterFilePath, 'utf-8');
    jsonData = fileContent ? JSON.parse(fileContent) : {};
  }

  if (
    jsonData["certificateReissueApiResponses"]
  ) {
    jsonData["certificateReissueApiResponses"] = {
      ...jsonData["certificateReissueApiResponses"],
      ...apiResponses
    };
  } else {
    jsonData["certificateReissueApiResponses"] = { ...apiResponses };
  }
  
  fs.writeFileSync(
    masterFilePath,
    JSON.stringify(jsonData, null, 2),
    'utf-8'
  );
}

module.exports = {
  updateTasksUsingPublicProject,
  criteriaValidation,
  updateCorruptedProjectsInDB,
  reIssueCertificates
};