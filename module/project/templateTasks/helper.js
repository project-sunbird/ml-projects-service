/**
 * name : helper.js
 * author : Aman
 * created-date : 22-July-2020
 * Description : Project templates tasks helper functionality.
 */

/**
    * ProjectTemplateTasksHelper
    * @class
*/

// Dependencies
const projectTemplatesHelper = require(MODULES_BASE_PATH + "/project/templates/helper");
const learningResourcesHelper = require(MODULES_BASE_PATH + "/learningResources/helper");
const surveyService = require(GENERICS_FILES_PATH + "/services/survey");
const projectTemplateTaskQueries = require(DB_QUERY_BASE_PATH + "/projectTemplateTask");
const projectTemplateQueries = require(DB_QUERY_BASE_PATH + "/projectTemplates");
const solutionsQueries = require(DB_QUERY_BASE_PATH + "/solutions");

module.exports = class ProjectTemplateTasksHelper {

    /**
     * Extract csv information.
     * @method
     * @name extractCsvInformation
     * @param {Array} csvData - csv data.
     * @param {String} projectTemplateId - project template id.
     * @returns {Array} Lists of tasks.
     */
    
    static extractCsvInformation(
        csvData,
        projectTemplateId
    ) {
        return new Promise(async (resolve, reject) => {
            try {

                let taskIds = [];
                let solutionIds = [];
                let systemId = false;
                let solutionExists = false;

                csvData.forEach(data => {
                    
                    let parsedData = UTILS.valueParser(data);

                    if( parsedData._SYSTEM_ID ) {
                        taskIds.push(parsedData._SYSTEM_ID);
                        systemId = true;
                    } else {
                        taskIds.push(parsedData.externalId);
                    }

                    if ( parsedData.solutionId && parsedData.solutionId !== "" ) {
                        solutionExists = true;
                        solutionIds.push(parsedData.solutionId);
                    }

                });

                let tasks = {};

                if( taskIds.length > 0 ) {

                    let filterData = {};

                    if( systemId ) {
                        filterData = {
                            _id : { $in : taskIds }
                        }
                    } else {
                        filterData = {
                            externalId : { $in : taskIds }
                        }
                    }

                    let tasksData = await projectTemplateTaskQueries.taskDocuments(
                        filterData,
                        ["_id","children","externalId","projectTemplateId","parentId", "taskSequence", "hasSubTasks"]
                    );

                    if( tasksData.length > 0 ) {
                        tasksData.forEach(task=> {
                            if( systemId ) {
                                tasks[task._id.toString()] = task;
                            } else {
                                tasks[task.externalId] = task;
                            }
                        });
                    }
                }

                let projectTemplate = 
                await projectTemplateQueries.templateDocument({
                    status : CONSTANTS.common.PUBLISHED,
                    _id : projectTemplateId
                },["_id","entityType","externalId", "taskSequence"]);

                if( !(projectTemplate.length > 0) ) {
                    throw {
                        message : CONSTANTS.apiResponses.PROJECT_TEMPLATE_NOT_FOUND,
                        status : HTTP_STATUS_CODE['bad_request'].status
                    }
                }

                if( solutionExists && !projectTemplate[0].entityType ) {
                    throw {
                        message : CONSTANTS.apiResponses.ENTITY_TYPE_NOT_FOUND_IN_TEMPLATE,
                        status : HTTP_STATUS_CODE['bad_request'].status
                    }
                }

                let solutionData = {};

                if ( solutionIds.length > 0 ) {
                    
                    let solutions = 
                    await surveyService.listSolutions(solutionIds);

                    if( !solutions.success ) {
                        throw {
                            message : CONSTANTS.apiResponses.SOLUTION_NOT_FOUND,
                            status : HTTP_STATUS_CODE['bad_request'].status
                        }
                    }

                    if ( 
                        solutions.data &&
                        Object.keys(solutions.data).length > 0 
                    ) {

                        solutions.data.forEach(solution => {
                            if(!solutionData[solution.externalId]) {
                                solutionData[solution.externalId] = solution;
                            }
                        });
                    }
                }

                return resolve({
                    success : true,
                    data : {
                        tasks : tasks,
                        template : projectTemplate[0],
                        solutionData : solutionData
                    }
                });

           } catch (error) {
               return resolve({
                   message : error.message,
                   success : false,
                   status : error.status ? error.status : HTTP_STATUS_CODE['internal_server_error'].status
               });
           }
       });
    }

    /**
     * Create a task.
     * @method
     * @name createOrUpdateTask
     * @param {Object} data - task data.
     * @param {String} templateId - template task id
     * @param {Object} solutionData - solution data
     * @param {String} [update = false]
     * @returns {Array} Create or update a task. 
     */
    
    static createOrUpdateTask( 
        data,
        template,
        solutionData,
        update = false
    ) {
        return new Promise(async (resolve, reject) => {
            try {

                let parsedData = UTILS.valueParser(data);

                let allValues = {
                    type : parsedData.type
                };

                let solutionTypes = [
                    CONSTANTS.common.ASSESSMENT,
                    CONSTANTS.common.OBSERVATION,
                    CONSTANTS.common.IMPROVEMENT_PROJECT
                ]

                if ( allValues.type === CONSTANTS.common.CONTENT ) {

                    let learningResources = 
                    await learningResourcesHelper.extractLearningResourcesFromCsv(parsedData);
                    
                    allValues.learningResources = learningResources.data;

                } else if ( solutionTypes.includes(allValues.type) ) { 

                    allValues.solutionDetails = {};
                    if( parsedData.solutionType && parsedData.solutionType !== "" ) {
                        allValues.solutionDetails.type = parsedData.solutionType; 
                    } else {
                        parsedData.STATUS = 
                        CONSTANTS.apiResponses.REQUIRED_SOLUTION_TYPE;
                    }
                    
                    if ( parsedData.solutionSubType && parsedData.solutionSubType !== "" ) {
                        allValues.solutionDetails.subType = parsedData.solutionSubType;
                    } else {
                        parsedData.STATUS = 
                        CONSTANTS.apiResponses.REQUIRED_SOLUTION_SUB_TYPE;
                    }

                    if ( parsedData.solutionId && parsedData.solutionId !== "" ) {

                        if ( !solutionData[parsedData.solutionId] ) {
                            parsedData.STATUS = 
                            CONSTANTS.apiResponses.SOLUTION_NOT_FOUND;
                        } else {

                            if( 
                                solutionData[parsedData.solutionId].type !== 
                                allValues.solutionDetails.type 
                            ) {
                                
                                parsedData.STATUS = 
                                CONSTANTS.apiResponses.SOLUTION_TYPE_MIS_MATCH;
                            }

                            if( 
                                solutionData[parsedData.solutionId].subType !== 
                                allValues.solutionDetails.subType
                            ) {
                                parsedData.STATUS = 
                                CONSTANTS.apiResponses.SOLUTION_SUB_TYPE_MIS_MATCH;
                            }

                            if( 
                                template.entityType !== solutionData[parsedData.solutionId].entityType 
                            ) {
                                parsedData.STATUS = 
                                CONSTANTS.apiResponses.MIS_MATCHED_PROJECT_AND_TASK_ENTITY_TYPE;
                            } else {

                                let projectionFields = _solutionDocumentProjectionFieldsForTask();
                                allValues.solutionDetails["minNoOfSubmissionsRequired"] = CONSTANTS.common.DEFAULT_SUBMISSION_REQUIRED;
                                
                                if (parsedData.minNoOfSubmissionsRequired && parsedData.minNoOfSubmissionsRequired != "" ) {

                                    // minNoOfSubmissionsRequired present in csv
                                    if ( parsedData.minNoOfSubmissionsRequired > CONSTANTS.common.DEFAULT_SUBMISSION_REQUIRED ) {
                                        if ( solutionData[parsedData.solutionId].allowMultipleAssessemts ) {
                                            allValues.solutionDetails["minNoOfSubmissionsRequired"] = parsedData.minNoOfSubmissionsRequired;
                                        } 
                                    }

                                }else{
                                    // minNoOfSubmissionsRequired not present in csv
                                    if (solutionData[parsedData.solutionId].minNoOfSubmissionsRequired ) {
                                        projectionFields.push("minNoOfSubmissionsRequired");
                                    }
                                }

                                Object.assign(allValues.solutionDetails, _.pick(
                                    solutionData[parsedData.solutionId],
                                    projectionFields
                                ))

                            }

                        }

                    } else {
                        parsedData.STATUS = 
                        CONSTANTS.apiResponses.REQUIRED_SOLUTION_ID;
                    }

                }

                allValues.projectTemplateId = template._id;
                allValues.projectTemplateExternalId = template.externalId;

                let templateTaskSchema = schemas["project-template-tasks"].schema;

                let templateTasksData = Object.keys(templateTaskSchema);
                let booleanData = UTILS.getAllBooleanDataFromModels(templateTaskSchema);

                Object.keys(parsedData).forEach( eachParsedData => {
                    if( 
                        templateTasksData.includes(eachParsedData) && 
                        !allValues[eachParsedData] 
                    ) {
                        if( booleanData.includes(eachParsedData) ) {
                            if( parsedData[eachParsedData] !== "" ) {
                                allValues[eachParsedData] = 
                                UTILS.convertStringToBoolean(parsedData[eachParsedData]);
                            }
                        } else {
                            allValues[eachParsedData] = parsedData[eachParsedData]; 
                        }
                    }
                });

                if( !parsedData.STATUS ) {

                    let taskData = {};

                    if( !update ) {
                        taskData = 
                        await projectTemplateTaskQueries.createTemplateTask(allValues);
                        if ( !taskData._id ) {
                            parsedData.STATUS = 
                            CONSTANTS.apiResponses.PROJECT_TEMPLATE_TASKS_NOT_CREATED;
                        } else {
                            parsedData._SYSTEM_ID = taskData._id;
                            parsedData.STATUS = CONSTANTS.apiResponses.SUCCESS;
                        }
    
                    } else {
                        
                        taskData = 
                        await projectTemplateTaskQueries.findOneAndUpdate({
                            _id :  parsedData._SYSTEM_ID
                        },{
                            $set : allValues
                        });

                        parsedData.STATUS = CONSTANTS.apiResponses.SUCCESS;
                    }
    
                    if( taskData._id ) {
                            
                        if( parsedData.hasAParentTask === "YES" ) { 
                    
                            let parentTask =
                            await projectTemplateTaskQueries.findOneAndUpdate({
                                externalId : parsedData.parentTaskId
                            },{
                                $addToSet : {
                                    children : taskData._id
                                },
                                $set : {
                                    hasSubTasks : true
                                }
                            },{
                                returnOriginal : true
                            });

                            if( parentTask._id ) {
                                
                                let visibleIf = [];
                                
                                let operator = 
                                parsedData["parentTaskOperator"] === "EQUALS" ? 
                                "===" : parsedData["parentQuestionOperator"];
                                
                                visibleIf.push({
                                    operator : operator,
                                    _id : parentTask._id,
                                    value : parsedData.parentTaskValue
                                });
                                
                                await projectTemplateTaskQueries.findOneAndUpdate({
                                    _id : taskData._id
                                },{
                                    $set : {
                                        parentId : parentTask._id,
                                        visibleIf : visibleIf
                                    }
                                });

                                if( update ) {
                                    parsedData._parentTaskId = parentTask._id;
                                }
                            }
                        }

                         //update solution project key 
                        if ( taskData.type == CONSTANTS.common.OBSERVATION && 
                            taskData.solutionDetails && 
                            taskData.solutionDetails._id 
                        ) {

                            let updateSolutionObj = {
                                "$set" : {}
                            };

                            updateSolutionObj["$set"]["referenceFrom"] = CONSTANTS.common.PROJECT;
                            updateSolutionObj["$set"]["project"] = {
                                _id: template._id.toString(),
                                taskId: taskData._id.toString()
                            };

                            await solutionsQueries.updateSolutionDocument
                            (
                                { _id : taskData.solutionDetails._id },
                                updateSolutionObj
                            )
                        }

                        //update project template
                        await projectTemplateQueries.updateProjectTemplateDocument
                        (
                            { _id : template._id },
                            { $addToSet : { tasks : ObjectId(taskData._id) } }
                        )
                    }
                }

                return resolve(
                    _.omit(parsedData,["createdBy","updatedBy"])
                );
           
           } catch (error) {
               return reject(error);
           }
       });
    }

    /**
      * Bulk create project template tasks.
      * @method
      * @name bulkCreate
      * @param {Array} tasks - csv tasks data.
      * @param {String} projectTemplateId - project template id.
      * @param {String} userId - user logged in id.
      * @returns {Object} Bulk create project template tasks.
     */

    static bulkCreate( tasks,projectTemplateId,userId ) {
        return new Promise(async (resolve, reject) => {
            try {

                const fileName = `create-project-template-tasks`;
                let fileStream = new CSV_FILE_STREAM(fileName);
                let input = fileStream.initStream();
      
                (async function () {
                    await fileStream.getProcessorPromise();
                    return resolve({
                        isResponseAStream: true,
                        fileNameWithPath: fileStream.fileNameWithPath()
                    });
                })();

                let csvData = 
                await this.extractCsvInformation(
                    tasks,
                    projectTemplateId
                );

                if( !csvData.success ) {
                    return resolve(csvData);
                }

                let pendingItems = [];
                let taskSequence = csvData.data.template.taskSequence && csvData.data.template.taskSequence.length > 0 
                    ? csvData.data.template.taskSequence: [];

                let checkMandatoryTask = [];

                for ( let task = 0; task < tasks.length ; task ++ ) {
                    let currentData = UTILS.valueParser(tasks[task]);
                    currentData.createdBy = currentData.updatedBy = userId;

                    if ( currentData.isDeletable != "" && currentData.isDeletable === "TRUE" ) {
                        checkMandatoryTask.push(currentData.externalId);
                    }

                    if( 
                        currentData["hasAParentTask"] === "YES" &&
                        !csvData.data.tasks[currentData.parentTaskId]
                    ) {

                        pendingItems.push(currentData);

                    } else {
                        
                        if( csvData.data.tasks[currentData.externalId] ) {
                            currentData._SYSTEM_ID = CONSTANTS.apiResponses.PROJECT_TEMPLATE_TASK_EXISTS;
                            input.push(currentData);
                        } else {
                            
                            let createdTask = 
                            await this.createOrUpdateTask(
                                currentData,
                                csvData.data.template,
                                csvData.data.solutionData
                            );

                            if (createdTask._SYSTEM_ID != ""){
                                taskSequence.push(createdTask.externalId);
                            }

                            input.push(createdTask);
                        }
                    }
                }

                let childTaskSequence = {};
                if ( pendingItems && pendingItems.length > 0 ) {
                    
                    for ( let item = 0; item < pendingItems.length ; item ++ ) {
                        
                        let currentData = pendingItems[item];

                        currentData.createdBy = currentData.updatedBy = userId;

                        if( csvData.data.tasks[currentData.externalId] ) {
                            currentData._SYSTEM_ID = CONSTANTS.apiResponses.PROJECT_TEMPLATE_TASK_EXISTS;
                            input.push(currentData);
                        } else {

                            let createdTask = await this.createOrUpdateTask(
                                currentData,
                                csvData.data.template,
                                csvData.data.solutionData,
                                csvData.data.observationData
                            );

                            if ( createdTask._SYSTEM_ID != "" ) {

                                if (!childTaskSequence.hasOwnProperty(currentData.parentTaskId)){
                                    childTaskSequence[currentData.parentTaskId] = new Array();
                                }
                                childTaskSequence[currentData.parentTaskId].push(currentData.externalId);
                            }

                            input.push(createdTask);
                        }

                    }
                }
        
                if ( taskSequence && taskSequence.length > 0 ) {
                    await projectTemplateQueries.updateProjectTemplateDocument
                        (
                            { _id : ObjectId(projectTemplateId) },
                            { $set : { taskSequence : taskSequence } }
                        )
                }

                if ( childTaskSequence && Object.keys(childTaskSequence).length > 0 ) {
                    for( let pointerToTask in childTaskSequence ) {
                        await projectTemplateTaskQueries.updateTaskDocument
                        (
                            { externalId : pointerToTask },
                            { $set : { taskSequence : childTaskSequence[pointerToTask] } }
                        )
                    }
                        
                }

                if ( checkMandatoryTask && checkMandatoryTask.length > 0 ) {

                    await this.checkAndUpdateParentTaskmandatory(checkMandatoryTask);
                    
                }

                input.push(null);

            } catch (error) {
                return reject(error);
            }
        })
    }

     /**
      * Bulk update project template tasks.
      * @method
      * @name bulkUpdate
      * @param {Array} tasks - csv tasks data.
      * @param {String} projectTemplateId - project template id.
      * @param {String} userId - user logged in id.
      * @returns {Object} Bulk update project template tasks.
     */

    static bulkUpdate( tasks,projectTemplateId,userId ) {
        return new Promise(async (resolve, reject) => {
            try {

                const fileName = `update-project-template-tasks`;
                let fileStream = new CSV_FILE_STREAM(fileName);
                let input = fileStream.initStream();
      
                (async function () {
                    await fileStream.getProcessorPromise();
                    return resolve({
                        isResponseAStream: true,
                        fileNameWithPath: fileStream.fileNameWithPath()
                    });
                })();

                let csvData = 
                await this.extractCsvInformation(
                    tasks,
                    projectTemplateId
                );

                if( !csvData.success ) {
                    return resolve(csvData);
                }

                let tasksData =  Object.values(csvData.data.tasks);

                if ( csvData.data.tasks && tasksData.length > 0 ) {

                    tasksData.forEach(task=>{
                        if ( task.children && task.children.length > 0 ) {
                            task.children.forEach(children=>{
                                if( csvData.data.tasks[children.toString()] ) {
                                    csvData.data.tasks[children.toString()].parentTaskId = task._id.toString();
                                }
                            })
                        }
                    })
                }

                let updateChildTaskSequence = {};
                let updateTemplateTaskSequence = new Array();
                let checkMandatoryTask = [];
                for ( let task = 0; task < tasks.length ; task ++ ) { 
                    
                    let currentData = UTILS.valueParser(tasks[task]);

                    if ( 
                        !currentData._SYSTEM_ID || 
                        !(currentData._SYSTEM_ID === "")|| 
                        !csvData.data.tasks[currentData["_SYSTEM_ID"]] 
                    ) {
                        currentData.STATUS = 
                        CONSTANTS.apiResponses.INVALID_TASK_ID;
                        input.push(currentData);
                        continue;
                    }

                    currentData.updatedBy = userId;

                    if ( currentData.isDeletable != "" && currentData.isDeletable === "TRUE" ) {
                        checkMandatoryTask.push(currentData.externalId);
                    }
                    
                    let createdTask = 
                    await this.createOrUpdateTask(
                        _.omit(currentData,["STATUS"]),
                        csvData.data.template,
                        csvData.data.solutionData,
                        true  
                    );

                    if ( createdTask._SYSTEM_ID != "") {

                        if ( currentData.parentTaskId != "" ) {
                            if ( !updateChildTaskSequence.hasOwnProperty(currentData.parentTaskId)){
                                updateChildTaskSequence[currentData.parentTaskId] = new Array();
                            }

                            updateChildTaskSequence[currentData.parentTaskId].push(currentData.externalId);
                        }else{
                            updateTemplateTaskSequence.push(currentData.externalId);
                        }
                    }

                    if( 
                        csvData.data.tasks[currentData._SYSTEM_ID].parentId && 
                        csvData.data.tasks[currentData._SYSTEM_ID].parentId.toString() !== createdTask._parentTaskId.toString()
                    ) {

                        await projectTemplateTaskQueries.findOneAndUpdate(
                            {
                              _id: csvData.data.tasks[currentData._SYSTEM_ID].parentId
                            },
                            {
                              $pull: { children : ObjectId(currentData._SYSTEM_ID) }
                            }
                          );

                    }

                    input.push(createdTask);
                }
                
                let checkTemplateTaskSequence = true;
                let templateTaskSequence = csvData.data.template.taskSequence;

                if ( templateTaskSequence ) {
                    checkTemplateTaskSequence = _.isEqual(templateTaskSequence, updateTemplateTaskSequence);
                }
                
                if ( updateTemplateTaskSequence && updateTemplateTaskSequence.length > 0 && checkTemplateTaskSequence == false ) {
                    await projectTemplateQueries.updateProjectTemplateDocument
                        (
                            { _id : ObjectId(projectTemplateId) },
                            { $set : { taskSequence : updateTemplateTaskSequence } }
                        )
                }
                
                if ( updateChildTaskSequence && Object.keys(updateChildTaskSequence).length > 0 ) {
                    for( let pointerToTask in updateChildTaskSequence ) {
                        await projectTemplateTaskQueries.updateTaskDocument
                        (
                            { externalId : pointerToTask },
                            { $set : { taskSequence : updateChildTaskSequence[pointerToTask] } }
                        )
                    }
                        
                }

                if ( checkMandatoryTask && checkMandatoryTask.length > 0 ) {

                    await this.checkAndUpdateParentTaskmandatory(checkMandatoryTask);
                    
                }

                input.push(null);

            } catch (error) {
                return reject(error);
            }
        })
    }

    /**
      * check parent task is mandatory.
      * @method
      * @name checkAndUpdateParentTaskmandatory
      * @param {Array} mandatoryTask - task external Ids.
      * @returns {Object} tasks.
     */

    static checkAndUpdateParentTaskmandatory( taskIds = [] ) {
        return new Promise(async (resolve, reject) => {
            try {

                let updateParentTask = [];

                let taskData = await projectTemplateTaskQueries.taskDocuments(
                    {   externalId : { $in : taskIds},
                        hasSubTasks : true
                    },
                    ["children"]
                );

                if ( taskData && taskData.length > 0 ) {

                    for ( let eachTask = 0 ; eachTask < taskData.length ; eachTask ++ ) {

                        let currentTask = taskData[eachTask];
                        if (currentTask.children && currentTask.children.length > 0) {

                            let childTasks = await projectTemplateTaskQueries.taskDocuments(
                                {   _id : { $in : currentTask.children}
                                },
                                ["isDeletable", "parentId"]
                            );

                            if ( childTasks && childTasks.length > 0 ) {

                                childTasks.forEach( eachChildTask => {
                                    if( eachChildTask.isDeletable  === false && eachChildTask.parentId != "" ) {
                                        updateParentTask.push(eachChildTask.parentId);
                                    } 
                                });
                            } 
                        }
                    }

                    if ( updateParentTask && updateParentTask.length > 0 ) {
                        let updatedTasks = await projectTemplateTaskQueries.updateTaskDocument
                        (
                            { _id : { $in : updateParentTask }},
                            { $set : { isDeletable : false } }
                        )
                    }
                    
                }

                return resolve({
                    success: true,
                    message: CONSTANTS.apiResponses.TASKS_MARKED_AS_ISDELETABLE_FALSE,
                    data : updateParentTask
                })

            } catch (error) {
                return reject(error);
            }
        })
    }

    /**
      * Task update.
      * @method
      * @name update
      * @param {String} taskId - Task id.
      * @param {Object} taskData - template task updation data
      * @param {String} userId - logged in user id.
      * @returns {Array} Project templates task data.
     */

    static update( taskId, taskData, userId ) {
        return new Promise(async (resolve, reject) => {
            try {

                let findQuery = {};

                let validateTaskId = UTILS.isValidMongoId(taskId);

                if( validateTaskId ) {
                  findQuery["_id"] = taskId;
                } else {
                  findQuery["externalId"] = taskId;
                }

                let taskDocument = await projectTemplateTaskQueries.taskDocuments(findQuery, ["_id"]);

                if ( !(taskDocument.length > 0) ) {
                    throw {
                        status : HTTP_STATUS_CODE.bad_request.status,
                        message : CONSTANTS.apiResponses.PROJECT_TEMPLATE_TASKS_NOT_FOUND
                    }
                }

                let updateObject = {
                    "$set" : {}
                };

                let taskUpdateData = taskData;

                Object.keys(taskUpdateData).forEach(updationData=>{
                    updateObject["$set"][updationData] = taskUpdateData[updationData];
                });

                updateObject["$set"]["updatedBy"] = userId;

                let taskUpdatedData = await projectTemplateTaskQueries.findOneAndUpdate({
                    _id :  taskDocument[0]._id
                }, updateObject, { new : true });

                if( !taskUpdatedData._id ) {
                    throw {
                      message : CONSTANTS.apiResponses.TEMPLATE_TASK_NOT_UPDATED
                    }
                }

                return resolve({
                    success : true,
                    data : taskUpdatedData,
                    message : CONSTANTS.apiResponses.PROJECT_TEMPLATE_TASK_UPDATED
                });
                
            } catch (error) {
                return reject(error);
            }
        })
    }

};

/**
    *  Helper function for list of solution fields to be sent in response.
    * @method
    * @name solutionDocumentProjectionFieldsForTask
    * @returns {Promise} Returns a Promise.
    */

function _solutionDocumentProjectionFieldsForTask() {

    let projectionFields = [
                "_id",
                "isReusable",
                "externalId",
                "name",
                "programId",
                "type",
                "subType",
                "allowMultipleAssessemts",
                "isRubricDriven",
                "criteriaLevelReport",
                "scoringSystem"
            ];

    return projectionFields;
}


