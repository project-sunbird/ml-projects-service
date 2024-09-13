/**
 * name : project-template-tasks.js.
 * author : Aman Karki.
 * created-date : 14-July-2020.
 * Description : Schema for project template tasks.
 */

module.exports = {
    name: "projectTemplateTasks",
    schema: {
        name : {
            type : String,
            required : true
        },
        description : {
            type : String
        },
        createdBy : {
            type : String,
            default : "SYSTEM"
        },
        updatedBy : {
            type : String,
            default : "SYSTEM"
        },
        isDeleted : {
            type : Boolean,
            default : false,
            index: true
        },
        externalId : {
            type : String,
            required : true,
            index: true,
            unique: true
        },
        type : {
            type : String,
            required : true
        },
        solutionDetails : {
            type : Object,
            default : {}
        },
        parentId : "ObjectId",
        projectTemplateId : {
            type : "ObjectId",
            required : true,
            index: true
        },
        projectTemplateExternalId : {
            type : String,
            required : true,
            index: true
        },
        isDeletable : {
            type : Boolean,
            default : true
        },
        taskSequence : {
            type : Array,
            default : []
        },
        children : {
            type : Array,
            default : []
        },
        improvementProjectDetails : {
            type : Object,
            default : {}
        },
        visibleIf : {
            type : Array,
            default : []
        },
        hasSubTasks : {
            type : Boolean,
            default : false
        },
        learningResources : {
            type : Array,
            default : []
        },
        sequenceNumber: {
            type : String
        }
    }
};