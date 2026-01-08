const mongoose = require('mongoose');
const fs = require("fs")
const path = require('path');
const { ObjectId } = require('mongodb');
const UTILS = require("../../generics/helpers/utils");
const request = require('request');



require('dotenv').config({
  path: path.resolve(__dirname, '../../.env'),
});

const MONGODB_URL = process.env.MONGODB_URL;

// --------------------
// Read input file
// --------------------
function getInputFileFromArgs() {
    const inputArg = process.argv.find(arg => arg.startsWith('--inputFile='));
    const batchSizeArg = process.argv.find(arg => arg.startsWith('--batchSize='));
  
    if (!inputArg) {
      console.error('❌ Missing --inputFile argument');
      console.error('👉 Usage: node script.js --inputFile=input.json');
      process.exit(1);
    }
  
    const inputFile = inputArg.split('=')[1];

    let batchSize = 100; // default

    if (batchSizeArg) {
      const parsed = parseInt(batchSizeArg.split('=')[1], 10);

      if (Number.isInteger(parsed) && parsed > 0) {
        batchSize = parsed;
      } else {
        console.warn(
          `⚠️ Invalid batchSize provided. Falling back to default (${batchSize})`
        );
      }
    }

    return {
      inputFile,
      batchSize
    };
}

const {inputFile , batchSize} = getInputFileFromArgs();

const inputFilePath = path.resolve(__dirname, inputFile);

if (!fs.existsSync(inputFilePath)) {
  console.error(`❌ Input file not found: ${inputFilePath}`);
  process.exit(1);
};

const inputData = JSON.parse(
  fs.readFileSync(inputFilePath, 'utf-8')
);
let solutions = [];
let corruptedProjects = {};
let validProjectPerSolution = {};

(inputData.summary || []).forEach(item => {
  if (item.componentId && !solutions.includes(item.componentId)) {
    solutions.push(item.componentId);
    corruptedProjects[item.componentId] = item.projectsCreatedDueToBug;
  }
});

const {
  userToken,
  projectServiceBaseUrl,
  writeMode
} = inputData;


const timestamp = new Date().toISOString().replace(/[:.]/g, '-');

const eligibleFilePath = path.resolve(
  __dirname,
  `eligibleProjects-${timestamp}.txt`
);

const nonEligibleFilePath = path.resolve(
  __dirname,
  `nonEligibleProjects-${timestamp}.txt`
);

const apiResponsesFilePath = path.resolve(
  __dirname,
  `certificateReissueApiResponses-${timestamp}.json`
);

// Create empty files (overwrite if they somehow exist)
fs.writeFileSync(eligibleFilePath, '', { flag: 'w' });
fs.writeFileSync(nonEligibleFilePath, '', { flag: 'w' });
fs.writeFileSync(apiResponsesFilePath, '', { flag: 'w' });


// --------------------
// MongoDB connection
// --------------------
async function connectDB() {
  if (!MONGODB_URL) {
    console.error('❌ MONGODB_URL is not defined in .env file');
    process.exit(1);
  }

  try {
    await mongoose.connect(MONGODB_URL, {
      useNewUrlParser: true,
      useUnifiedTopology: true,
    });

    console.log('✅ MongoDB connected successfully');
  } catch (error) {
    console.error('❌ MongoDB connection error:', error.message);
    throw error; // let caller decide what to do
  }
}

function chunkArray(array) {
    const chunks = [];
    for (let i = 0; i < array.length; i += batchSize) {
      chunks.push(array.slice(i, i + batchSize));
    }
    return chunks;
  }
  

// --------------------
// Fetch projects
// --------------------
async function fetchProjectsFromDB(projectIds) {
  
    const projectsCollection = mongoose.connection.collection('projects');
    projectIds = projectIds.filter(id => ObjectId.isValid(id))
                            .map(id => new ObjectId(id));
    if (!projectIds.length) return [];

    const projects = await projectsCollection
      .find(
        { 
            _id: { $in: projectIds },
            status: "submitted",
            certificate: { $exists: true }
        },
        {
          projection : {
            _id : 1,
            certificate : 1,
            status : 1,
            solutionId : 1,
            tasks : 1,
            attachments : 1
          }
        }
      )
      .toArray();
  
    return projects;
  }


// ------------------------
// Fetch valid projects
// ------------------------
async function fetchValidProjectsFromDB(solutions) {
    const projectsCollection = mongoose.connection.collection('projects');
    solutions = solutions.filter(id => ObjectId.isValid(id));
    for (let i = 0; i < solutions.length; i++) {
        const validProject = await projectsCollection
        .findOne(
            {
                solutionId: solutions[i],
                isAPrivateProgram: false,
                isMigratedDueToReportIssue: { $exists: false },
            },
            {
                projection: {
                    _id: 1,
                    solutionId: 1,
                    tasks: 1
                },
            }
        );
        validProjectPerSolution[solutions[i]] = validProject;
    }
}


function updateTasksUsingValidProject(projects, solutionId) {
  const validProject = validProjectPerSolution[solutionId];
  if (
    !validProject ||
    !Array.isArray(validProject.tasks) ||
    validProject.tasks.length === 0
  ) {
    return;
  }
  const referenceTasks = validProject.tasks;

  for (const project of projects) {
    if (!project || !Array.isArray(project.tasks)) continue;

    // Loop through reference project tasks
    for (const refTask of referenceTasks) {
      if (!refTask || !refTask.externalId) continue;

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
}


async function updateProjectsWithTasks(projects) {
  if (!writeMode) {
    console.log('WriteMode disabled - skipped DB update');
    return;
  }

  if (!Array.isArray(projects) || !projects.length) {
    return;
  }

  const projectsCollection = mongoose.connection.collection('projects');

  const bulkOps = [];

  for (const project of projects) {
    if (
      !project ||
      !project._id ||
      !Array.isArray(project.tasks)
    ) {
      continue;
    }

    bulkOps.push({
      updateOne: {
        filter: { _id: project._id },
        update: {
          $set: { tasks: project.tasks, isMigratedDueToReportIssue: true }
        }
      }
    });
  }

  if (!bulkOps.length) {
    console.log('No valid projects found for update');
    return;
  }

  const result = await projectsCollection.bulkWrite(bulkOps, {
    ordered: false
  });

  console.log({
    matched: result.matchedCount,
    modified: result.modifiedCount
  });
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

// ------------------------
// Update eligible projects
// ------------------------
async function updateEligibleProjects(projects) {
  if (!writeMode) {
    console.log('WriteMode disabled - skipped DB update');
    return;
  }

  if (!Array.isArray(projects) || projects.length === 0) {
    console.log('No projects to update eligibility');
    return;
  }

  const projectsCollection = mongoose.connection.collection('projects');

  const bulkOps = [];
  const eligibleIds = [];
  const nonEligibleIds = [];

  for (const project of projects) {
    if (!project || !project._id) continue;

    const projectId = project._id.toString();

    if (project.eligible === true) {
      eligibleIds.push(projectId);

      bulkOps.push({
        updateOne: {
          filter: { _id: project._id },
          update: {
            $set: { "certificate.eligible": true }
          }
        }
      });
    } else {
      nonEligibleIds.push(projectId);
    }
  }

  // Append projectIds to files (NOT overwrite)
  if (eligibleIds.length) {
    fs.appendFileSync(
      eligibleFilePath,
      eligibleIds.join('\n') + '\n',
      'utf8'
    );
  }

  if (nonEligibleIds.length) {
    fs.appendFileSync(
      nonEligibleFilePath,
      nonEligibleIds.join('\n') + '\n',
      'utf8'
    );
  }


  if (!bulkOps.length) {
    console.log('No eligible projects found to update');
    return;
  }

  const result = await projectsCollection.bulkWrite(bulkOps, {
    ordered: false
  });

  console.log({
    eligibleUpdated: result.modifiedCount
  });
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

      const statusCode = response ? response.statusCode : 500;

      if (statusCode >= 200 && statusCode < 300) {
        return resolve(body);
      }

      reject({
        statusCode,
        body
      });
    });
  });
}

  
  
async function reIssueCertificates(projects) {
  if (!writeMode) {
    console.log("WriteMode disabled. Skipped reissuing certificates");
    return;
  }

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
        url: `${projectServiceBaseUrl}/userProjects/certificateReIssue/${projectId}`,
        headers: {
          'x-authenticated-user-token': userToken
        },
        json: true
      });

      apiResponses[projectId] = responseBody ?? null

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

  let existing = [];

  if (fs.existsSync(apiResponsesFilePath)) {
    const content = fs.readFileSync(apiResponsesFilePath, 'utf-8').trim();
    if (content) {
      existing = JSON.parse(content);
    }
  }
  
  existing.push(apiResponses);
  
  fs.writeFileSync(
    apiResponsesFilePath,
    JSON.stringify(existing, null, 2),
    'utf-8'
  );

  console.log(`📄 API responses written to ${apiResponsesFilePath}`);
}

  



async function runMigration() {
  try {
    // Check MongoDB connectivity
    await connectDB();

    // fetch validProjects per solution
    await fetchValidProjectsFromDB(solutions)

    for (const [solutionId, projectIds] of Object.entries(corruptedProjects)) {
      console.log(`Processing component: ${solutionId}`);
      if(!validProjectPerSolution[solutionId]){
        console.log("No valid project found for solutionId", solutionId);
        continue;
      }
      const batches = chunkArray(projectIds);
  
      for (const batch of batches) {

        //migration logic
        const projects = await fetchProjectsFromDB(batch);

        // update tasks using reference projects
        updateTasksUsingValidProject(projects, solutionId);

        // persist changes to DB
        await updateProjectsWithTasks(projects);

        // check project's eligibility
        for (const project of projects) {
          try {
            const validationResult = await criteriaValidation(project);
        
            project.eligible = validationResult && validationResult.success === true;
          } catch (error) {
            project.eligible = false;
        
            console.error(
              `❌ Criteria validation failed for project ${project._id.toString()}`,
              error.message || error
            );
          }
        }        
        // update eligible projects
        await updateEligibleProjects(
          projects
        );

        // reissue certificates
        await reIssueCertificates(projects);
      }
    }    
    
  } catch (error) {
    console.error('❌ Migration aborted');
    process.exit(1);
  } finally {
    // Always close connection for scripts
    await mongoose.connection.close();
    console.log('🔌 MongoDB connection closed');
  }
}

runMigration();