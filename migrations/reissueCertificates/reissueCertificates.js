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
  
    if (!inputArg) {
      console.error('❌ Missing --inputFile argument');
      console.error('👉 Usage: node script.js --inputFile=input.json');
      process.exit(1);
    }
  
    return inputArg.split('=')[1];
}

const inputFileName = getInputFileFromArgs();

const inputFilePath = path.resolve(__dirname, inputFileName);

if (!fs.existsSync(inputFilePath)) {
  console.error(`❌ Input file not found: ${inputFilePath}`);
  process.exit(1);
}

const inputData = JSON.parse(
  fs.readFileSync(inputFilePath, 'utf-8')
);
const batchSize = 100

const {
  userToken,
  projectServiceBaseUrl,
  writeMode,
  projectIds,
} = inputData;

const projectDataMap = {};
const referenceProjectMap = {};

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
    const results = [];

    const batches = chunkArray(projectIds);
  
    for (let i = 0; i < batches.length; i++) {
        const batch = batches[i]
        .filter(id => ObjectId.isValid(id))
        .map(id => new ObjectId(id));

        if (!batch.length) continue;
        const projects = await projectsCollection
          .find(
            { 
                _id: { $in: batch },
                status: "submitted",
                certificate: { $exists: true }
            }
          )
          .toArray();
    
        results.push(...projects);    
        console.log(`📦 Batch ${i + 1}/${batches.length} fetched (${projects.length} projects)`);
      }
  
    return results;
  }


// ------------------------
// Fetch referene projects
// ------------------------
async function fetchReferenceProjectsInBatches(solutionIds) {
    const projectsCollection = mongoose.connection.collection('projects');
    const results = [];

    const batches = chunkArray(solutionIds);
    for (let i = 0; i < batches.length; i++) {
        const batch = batches[i]

        const referenceProjects = await projectsCollection
        .find(
            {
                solutionId: { $in: batch },
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
        )
        .toArray();

        results.push(...referenceProjects);

        console.log(`📦 Reference batch ${i + 1}/${batches.length} fetched (${referenceProjects.length})`);
    }

    return results;
}


function updateTasksUsingReferenceProject(projectDataMap) {
    for (const projectId of Object.keys(projectDataMap)) {
      const project = projectDataMap[projectId];
      const referenceProject = project.referenceProject;
  
      // Validate reference project and tasks
      if (
        !referenceProject ||
        !Array.isArray(referenceProject.tasks) ||
        referenceProject.tasks.length === 0 ||
        !Array.isArray(project.tasks)
      ) {
        continue;
      }
  
      // Loop through reference project tasks
      for (const refTask of referenceProject.tasks) {
        if (!refTask || !refTask.externalId) continue;

        // Prefix-based match
        const lastDashIndex = refTask.externalId.lastIndexOf('-');

        if (lastDashIndex > -1) {
            const prefix = refTask.externalId.substring(0, lastDashIndex);
            // Loop through migrated project tasks
            for (const task of project.tasks) {
                if (!task || !task.externalId) continue;    
    
                if (task.externalId.startsWith(prefix)) {
                    task.referenceId = refTask.referenceId;
                    task.externalId = refTask.externalId;
                    break;
                }
            }
        }
      }
    }
  }

async function updateProjectsWithTasks(
    projectDataMap,
    writeMode = false
  ) {
    if(!writeMode){
        console.log('WriteMode disbaled - skipped DB update')
        return;
    }

    const projectsCollection = mongoose.connection.collection('projects');
  
    const projectEntries = Object.values(projectDataMap);
  
    const batches = [];
    for (let i = 0; i < projectEntries.length; i += batchSize) {
      batches.push(projectEntries.slice(i, i + batchSize));
    }
  
    for (let i = 0; i < batches.length; i++) {
      const batch = batches[i];
      const bulkOps = [];
  
      for (const project of batch) {
        bulkOps.push({
          updateOne: {
            filter: { _id: new ObjectId(project.projectId) },
            update: {
              $set: { tasks: project.tasks },
            },
          },
        });
      }
  
      if (!bulkOps.length) continue;
   
      const result = await projectsCollection.bulkWrite(bulkOps, {
        ordered: false,
      });
  
      console.log(
        `Tasks Update:- Batch ${i + 1}/${batches.length} updated`,
        {
          matched: result.matchedCount,
          modified: result.modifiedCount,
        }
      );
    }
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
async function updateEligibleProjects(
    projectDataMap,
    writeMode = false
  ) {
    if(!writeMode){
        console.log('WriteMode disbaled - skipped DB update')
        return;
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');
    const updatedProjectsFilePath = path.resolve(
      __dirname,
      `updatedProjects-${timestamp}.txt`
    );
    fs.writeFileSync(updatedProjectsFilePath, '', { flag: 'w' });

    const nonUpdatedProjectsFilePath = path.resolve(
        __dirname,
        `nonEligibleProjects-${timestamp}.txt`
      );
    fs.writeFileSync(nonUpdatedProjectsFilePath, '', { flag: 'w' });
  
      
    const projectsCollection = mongoose.connection.collection('projects');
  
    // filter only eligible projects
    const eligibleProjects = Object.values(projectDataMap).filter(
      project => project.eligible === true
    );

    // filter non eligible projects
    const nonEligibleProjects = Object.values(projectDataMap).filter(
        project => project.eligible === false
    );

    // write non-eligible project IDs to file
    if (nonEligibleProjects.length) {
        const dataToWrite =
        nonEligibleProjects
            .map(project => project.projectId)
            .join('\n') + '\n';
    
        fs.appendFileSync(nonUpdatedProjectsFilePath, dataToWrite);
    
        console.log(
        `📄 Non-eligible project IDs written to ${nonUpdatedProjectsFilePath}`
        );
    } else {
        console.log('✅ No non-eligible projects found');
    }
  
    if (!eligibleProjects.length) {
      console.log('No eligible projects to update');
      return;
    }
  
    // create batches
    for (let i = 0; i < eligibleProjects.length; i += batchSize) {
      const batch = eligibleProjects.slice(i, i + batchSize);
      const bulkOps = [];
  
      for (const project of batch) {
        bulkOps.push({
          updateOne: {
            filter: { _id: new ObjectId(project.projectId) },
            update: {
              $set: {
                tasks: project.tasks,
                'certificate.eligible': true,
                updatedAt: new Date(),
                isMigratedDueToReportIssue: true
              },
            },
          },
        });
      }
  
      const result = await projectsCollection.bulkWrite(bulkOps, {
        ordered: false,
      });

    // ✅ Append updated projectIds to file
    const projectIdsToWrite = batch
    .map(project => project.projectId)
    .join('\n') + '\n';

    fs.appendFileSync(updatedProjectsFilePath, projectIdsToWrite);
    console.log(`📄 Updated projects written to ${updatedProjectsFilePath}`);
  
      console.log(`Eligibility Update:- Batch ${Math.floor(i / batchSize) + 1} updated`,{
          matched: result.matchedCount,
          modified: result.modifiedCount,
        }
      );
    }
}

function requestPromise(options) {
    return new Promise((resolve, reject) => {
      request(options, (error, response, body) => {
        if (error) {
          return reject(error);
        }
  
        const statusCode = response? response.statusCode : 500
  
        if (statusCode >= 200 && statusCode < 300) {
          return resolve(body);
        }
  
        reject({
          statusCode,
          body,
        });
      });
    });
  }
  
  

async function reIssueCertificates(projectDataMap) {
    if(!writeMode){
        console.log("WriteMode disabled. Skipped reissuing certificates");
        return;
    }

    const timestamp = new Date().toISOString().replace(/[:.]/g, '-');

    const apiResponsesFilePath = path.resolve(
      __dirname,
      `apiResponses-${timestamp}.json`
    );
  
    const apiResponses = {};
  
    for (const project of Object.values(projectDataMap)) {
      if (project.eligible !== true) continue;
      try {
        const responseBody = await requestPromise({
            method: 'POST',
            url: `${projectServiceBaseUrl}/userProjects/certificateReIssue/${project.projectId}`,
            headers: {
              'x-authenticated-user-token': userToken,
              'Content-Type': 'application/json',
            },
            json: true, // auto parses JSON response
          });
      
        apiResponses[project.projectId] = {
            success: true,
            response: responseBody? responseBody : null,
        };
  
        console.log(`✅ API success for project ${project.projectId}`);
      } catch (error) {
        apiResponses[project.projectId] = {
          success: false,
          error: error.body || error.message
        };
  
        console.error(`❌ API failed for project ${project.projectId}`);
      }
    }
  
    // write responses to file
    if (writeMode) {
      fs.writeFileSync(
        apiResponsesFilePath,
        JSON.stringify(apiResponses, null, 2),
        'utf-8'
      );
  
      console.log(`📄 API responses written to ${apiResponsesFilePath}`);
    } else {
      console.log('🧪 Dry run — API responses not written to file');
    }
  
    return apiResponses;
}
  



async function runMigration() {
  try {
    // Check MongoDB connectivity
    await connectDB();

    //migration logic
    const projects = await fetchProjectsFromDB(projectIds);

    // add projects data to projectDataMap fo future reference
    for (const project of projects) {
        // Skip projects without certificate
        if (!project.certificate) {
          continue;
        }
      
        projectDataMap[project._id.toString()] = {
          projectId: project._id.toString(),
          solutionId: project.solutionId || null,
          tasks: project.tasks || [],
          certificate: project.certificate,
          attachments: project.attachments,
          status: project.status
        };
      }
    console.log('📦 Project data fetched successfully');

    // gather unique solutionIds
    const solutionIds = [...new Set(
        Object.values(projectDataMap)
          .map(p => p.solutionId)
          .filter(Boolean)
    )];

    // fetch valid reference projects 
    const referenceProjects = await fetchReferenceProjectsInBatches(solutionIds);
    for (const ref of referenceProjects) {
        // pick first one if multiple exist for same solutionId
        if (!referenceProjectMap[ref.solutionId]) {
          referenceProjectMap[ref.solutionId] = {
            projectId: ref._id.toString(),
            solutionId: ref.solutionId,
            tasks: ref.tasks
          };
        }
    }

    // attach reference project to projectDataMap
    for (const projectId of Object.keys(projectDataMap)) {
        const solutionId = projectDataMap[projectId].solutionId;      
        projectDataMap[projectId].referenceProject = referenceProjectMap[solutionId] || null;
    }

    // remove projects who dont have a referenceProject
    for (const projectId of Object.keys(projectDataMap)) {
        if (!projectDataMap[projectId].referenceProject) {
          delete projectDataMap[projectId];
        }
    }      
      
    // update tasks using reference projects
    updateTasksUsingReferenceProject(projectDataMap);
    // persist changes to DB
    await updateProjectsWithTasks(projectDataMap, writeMode);
    console.log('✅ Task referenceIds updated successfully');

    // check project's eligibility
    for (const projectId of Object.keys(projectDataMap)) {
        const project = projectDataMap[projectId];
        
        try {
            const validationResult = await criteriaValidation(project);
            
            projectDataMap[projectId].eligible =
            validationResult && validationResult.success === true;
        } catch (error) {
            // If validation fails unexpectedly, mark as not eligible
          projectDataMap[projectId].eligible = false;
          
          console.error(
              `❌ Criteria validation failed for project ${projectId}`,
              error.message || error
            );
        }
    }

    // update eligible projects
    await updateEligibleProjects(
        projectDataMap,
        writeMode
    );
    
    // reissue certificates
    await reIssueCertificates(projectDataMap);
    
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