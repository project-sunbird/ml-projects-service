/**
 * name : updatePrivateProgramInProject.js
 * author : Ankit Shahu
 * created-date : 02-Feb-2023
 * Description : Migration script for update project
 */

 const path = require("path");
 let rootPath = path.join(__dirname, '../../')
 require('dotenv').config({ path: rootPath+'/.env' })
 
 let _ = require("lodash");
 let mongoUrl = process.env.MONGODB_URL;
 let dbName = mongoUrl.split("/").pop();
 let url = mongoUrl.split(dbName)[0];
 var MongoClient = require('mongodb').MongoClient;
 var ObjectId = require('mongodb').ObjectID;
 
 var fs = require('fs');


(async () => {

    let connection = await MongoClient.connect(url, { useNewUrlParser: true });
    let db = connection.db(dbName);
    try {

        let updatedProjectIds = [];
        let deletedSolutionIds = [];
        let deletedProgramIds = [];
        

        
        //get all projectss id where user profile is not there.
        let projectDocument = await db.collection('projects').find({
            userRoleInformation: {$exists : false},
            isAPrivateProgram: true,
        }).project({_id:1,userProfile:1}).toArray();


        let chunkOfProjectDocument = _.chunk(projectDocument, 10);
        // console.log(chunkOfProjectDocument)
        let projectIds;

        for (let pointerToProject = 0; pointerToProject < chunkOfProjectDocument.length; pointerToProject++) {
            projectIds = await chunkOfProjectDocument[pointerToProject].map(
                projectDoc => {
                  return projectDoc._id;
                }
            );


            // get project documents from projectss collection in Array
            let projectDocuments = await db.collection('projects').find({
                _id: { $in :projectIds  }
            }).project({_id:1,userProfile:1}).toArray();
            //iterate project documents one by one
            for(let counter = 0; counter < projectDocuments.length; counter++) {

               
                if(projectDocuments[counter].hasOwnProperty("solutionId") && projectDocuments[counter].isAPrivateProgram){
                    // find solution document form solution collection
                    let solutionDocument = await db.collection('solutions').find({
                        _id: projectDocuments[counter].solutionId,
                        parentSolutionId : {$exists:true},
                        isAPrivateProgram : true
                    }).project({}).toArray({})
                    //find program document form program collection
                    if(solutionDocument.length == 1){
                       
                        // find parent solution document in same collection
                        let parentSolutionDocument = await db.collection('solutions').find({
                            _id: solutionDocument[0].parentSolutionId}).project({}).toArray({});
                        //varibale to update project document
                        let updateProjectDocument = {
                            "$set" : {}
                        };
                        updateProjectDocument["$set"]["solutionId"] = parentSolutionDocument[0]._id
                        updateProjectDocument["$set"]["isAPrivateProgram"] = parentSolutionDocument[0].isAPrivateProgram
                        updateProjectDocument["$set"]["solutionInformation"] = {
                            name: parentSolutionDocument[0].name,   
                            description: parentSolutionDocument[0].description,
                            externalId: parentSolutionDocument[0].externalId,
                            _id: parentSolutionDocument[0]._id,
                        }
                        updateProjectDocument["$set"]["solutionExternalId"] = parentSolutionDocument[0].externalId,
                        updateProjectDocument["$set"]["programId"] = parentSolutionDocument[0].programId,
                        updateProjectDocument["$set"]["programExternalId"] =  parentSolutionDocument[0].programExternalId
                        updateProjectDocument["$set"]["programInformation"] = {
                            _id : parentSolutionDocument[0].programId,
                            name : parentSolutionDocument[0].programName,
                            externalId : parentSolutionDocument[0].programExternalId,
                            description : parentSolutionDocument[0].programDescription,
                            isAPrivateProgram : parentSolutionDocument[0].isAPrivateProgram
                        }
                        if(projectDocuments[counter].hasOwnProperty("userProfile"))
                        {
                            let userLocations = projectDocuments[counter].userProfile.userLocations
                            let userRoleInfomration = {}
                            //get data in userRoleInfomration key 
                            for(let userLocationCounter = 0; userLocationCounter < userLocations.length; userLocationCounter++){
                                if(userLocations[userLocationCounter].type !== "school"){
                                    userRoleInfomration[userLocations[userLocationCounter].type] = userLocations[userLocationCounter].id
                                }else{
                                    userRoleInfomration[userLocations[userLocationCounter].type] = userLocations[userLocationCounter].code
                                }
                            }
                            let Roles = ""
                            for(let roleCounter = 0; roleCounter < projectDocuments[counter].userProfile.profileUserTypes.length; roleCounter++){
                                Roles = Roles !== "" ? Roles+"," : Roles
                                Roles += (projectDocuments[counter].userProfile.profileUserTypes[roleCounter].subType ? projectDocuments[counter].userProfile.profileUserTypes[roleCounter].subType.toUpperCase() : projectDocuments[counter].userProfile.profileUserTypes[roleCounter].type.toUpperCase())
                            }
                            userRoleInfomration.Role = Roles
                            updateProjectDocument["$set"]["userRoleInformation"] = userRoleInfomration
                        }

                        //push all updated and deleted id in arrays and save in file
                        updatedProjectIds.push(projectDocuments[counter]._id)
                        deletedSolutionIds.push(projectDocuments[counter].solutionId)
                        deletedProgramIds.push(projectDocuments[counter].programId)

                        // update project documents 
                        await db.collection('projects').findOneAndUpdate({
                            "_id" : projectDocuments[counter]._id
                        },updateProjectDocument);

                        await db.collection('solutions').deleteOne({
                            _id: projectDocuments[counter].solutionId
                        })
                        await db.collection('programs').deleteOne({
                            _id: projectDocuments[counter].programId
                        })
                    }
                }
            }

            
           
            


            

            //write updated project ids to file
            fs.writeFile(
                'updatedProjectIdsAll.json',

                JSON.stringify({updatedProjectIds: updatedProjectIds,deletedProgramIds: deletedProgramIds,deletedSolutionIds: deletedSolutionIds}),

                function (err) {
                    if (err) {
                        console.error('Crap happens');
                    }
                }
            );
        }
        console.log("Updated Project Count : ", updatedProjectIds.length)
        console.log("deleted program Count : ", deletedProgramIds.length)
        console.log("deleted solutionId Count : ", deletedSolutionIds.length)
        console.log("completed")
        connection.close();
    }
    catch (error) {
        console.log(error)
    }
})().catch(err => console.error(err));