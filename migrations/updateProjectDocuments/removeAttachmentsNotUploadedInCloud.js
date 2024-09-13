/**
 * name : updatePrivateProgramInProject.js
 * author : Ankit Shahu
 * created-date : 02-Feb-2023
 * Description : Migration script for update project
 */
 "use strict";
const path = require("path");
let rootPath = path.join(__dirname, '../../')
require('dotenv').config({ path: rootPath+'/.env' })

let _ = require("lodash");
let mongoUrl = process.env.MONGODB_URL;
let dbName = mongoUrl.split("/").pop();
let url = mongoUrl.split(dbName)[0];
var MongoClient = require('mongodb').MongoClient;
var ObjectId = require('mongodb').ObjectID;
var request = require('request');
var fs = require('fs');
const { at } = require("lodash");

const filePathUrl = "https://samikshaprod.blob.core.windows.net/samiksha/";


(async () => {
    
    let connection = await MongoClient.connect(url, { useNewUrlParser: true });
    let db = connection.db(dbName);
    try {
        // check project attachments for isUploaded = false data and remove the attachment object
        let collectionDocs = await db.collection('projects').find({    
            "$or": [
                {
                    "attachments.isUploaded": false 
                },
                {
                   "tasks.attachments.isUploaded": false
                }
            ]
        }).project({_id:1}).toArray();
  
        //varibale to store all projectIds which are updated
        let projectIds = [];
        collectionDocs.forEach( eachDoc => {
            projectIds.push(eachDoc._id);
        })
      
        let chunkOfProjectIds = _.chunk(projectIds, 10);
        let UpdatedProjectId = []
        //loop project chunks
        for ( let chunkPointer = 0; chunkPointer < chunkOfProjectIds.length; chunkPointer++ ) {
           
            //chunk of project ids
            let projectId = chunkOfProjectIds[chunkPointer];

            //loop for chunk of project id in chunk
            for ( let projectIdpointer = 0 ; projectIdpointer < projectId.length; projectIdpointer++ ) {
                let id = projectId[projectIdpointer];
                
                //pull project Data from DB 
                let pullProjectDataDB = await db.collection('projects').find({
                    _id: id
                }).project({}).toArray({})

                //store in varibale to avoid using [0]
                const pullProjectData = pullProjectDataDB[0]

                const allTasksBeforeValidation = JSON.stringify(pullProjectData.tasks)
                let allAttachmentsBeforeValidation = ""

                //update Object
                let updateObject = {
                    "$set" : {}
                }

                //check if project Document has attachments or not if present then checks length of attachment it should be greater than 0
                if( pullProjectData.hasOwnProperty('attachments') && pullProjectData.attachments.length > 0 ) { 
                    allAttachmentsBeforeValidation = JSON.stringify(pullProjectData.attachments)
                    //assign attachmentPresent object to update variable
                    updateObject['$set']['attachments'] = await validatedAttachments(pullProjectData.attachments)
                }

                //check if project has tasks available and length of tasks array should be greater than 0   
                if( pullProjectData.hasOwnProperty('tasks') && pullProjectData.tasks.length > 0){

                    let tasks = pullProjectData.tasks
                    updateObject['$set']['tasks'] = [...tasks]
                    
                    //for loop for each task
                    for(let taskCounter = 0; taskCounter< tasks.length; taskCounter++){

                        //checks if task object has key attachment present or not
                        if(tasks[taskCounter].hasOwnProperty("attachments") && tasks[taskCounter].attachments.length>0){
                            //assign attachmentPresent array to task attachments
                            updateObject['$set']['tasks'][taskCounter].attachments = await validatedAttachments((updateObject['$set']['tasks'][taskCounter].attachments))
                        }
                    }
                }
            
                //push project id which is validated
                if(JSON.stringify(updateObject["$set"]["tasks"]) != allTasksBeforeValidation || JSON.stringify(updateObject["$set"]["attachments"]) != allAttachmentsBeforeValidation){
                  
                    if(JSON.stringify(updateObject["$set"]["tasks"]) == allTasksBeforeValidation){
                        delete updateObject["$set"].tasks
                    }
                    if(JSON.stringify(updateObject["$set"]["attachments"]) == allAttachmentsBeforeValidation){
                        delete updateObject["$set"].attachments
                    }
                    if(updateObject["$set"].hasOwnProperty("attachments") || updateObject["$set"].hasOwnProperty("tasks")){
                    UpdatedProjectId.push(id)
                    //update project with new varibales
                    await db.collection('projects').updateOne({_id:id},updateObject)}
                }
            }
            
            
        }
        console.log(UpdatedProjectId)

       

        fs.writeFile(
            'updatedProjectWithAttachments.json',

            JSON.stringify({updatedProjectIds: UpdatedProjectId}),

            function (err) {
                if (err) {
                    console.error('Crap happens');
                }
            }
        );



        async function validatedAttachments(attachments){
            //varibale to store updated attachment objects
            let attachmentsPresent = []
        
            //for loop for each attachment
            for(let attachmentCounter = 0; attachmentCounter < attachments.length; attachmentCounter++){
        
                //check if isUploaded key is present or not and isUploaded should be false and checks type of attachment 
                if(attachments[attachmentCounter].hasOwnProperty("isUploaded") && !attachments[attachmentCounter].isUploaded && attachments[attachmentCounter].type !== "link"){
        
                    //checks if document is present or not
                    let documentExist = await getDocumentStatus(attachments[attachmentCounter].sourcePath)
                    //if present then push object to new array and update isUploaded to true
                    if(documentExist.success){
                        attachments[attachmentCounter].isUploaded = true
                        attachmentsPresent.push(attachments[attachmentCounter])
                    }
                }else{
                    //if isUploaded is not present then push object to new array and if type is link then also
                    attachmentsPresent.push(attachments[attachmentCounter])
                }
            }
        
            return attachmentsPresent;
        }
        
        //function to check if document exists
        function getDocumentStatus (sourcePath) {
            return new Promise(async (resolve, reject) => {
                try {
                    let url = filePathUrl + sourcePath;
                    const options = {
                        headers : {
                        }
                    };
                    request.get(url,options,userReadCallback);
                    let result = {
                        success : true
                    };
                    function userReadCallback(err, data) { 
                        if (err) {
                            result.success = false;
                        } else {
                            if( data.statusCode === 200 ) {
                                result.success = true;
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
                     }, 5000);
        
                } catch (error) {
                    return reject(error);
                }
            })
        }
        
        
        console.log("Updated Projects", UpdatedProjectId.length)
        console.log("finished project attachment deletion based on isUploaded:= false, completed...")
        connection.close();
    }
    catch (error) {
        console.log(error)
    }
})().catch(err => console.error(err));


