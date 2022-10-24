/**
 * name : project-templates.js.
 * author : Aman Karki.
 * created-date : 13-July-2020.
 * Description : Schema for project templates.
 */
module.exports = {
    name: "projectTemplates",
    schema: {
        title : {
            type : String,
            required : true,
            text : true
        },
        externalId : {
            type : String,
            required : true,
            index: true,
            unique: true
        },
        categories : [{
            _id : "ObjectId",
            externalId : {
                type : String,
                index : true
            },
            name : String
        }],
        description : {
            type : String,
            default : ""
        },
        concepts : {
            type : Array,
            default : []
        },
        keywords : {
            type : Array,
            default : []
        },
        status : {
            type : String,
            required : true,
            index : true
        },
        isDeleted : {
            type : Boolean,
            default : false,
            index : true
        },
        recommendedFor : {
            type : Array,
            default : [] 
        },
        tasks : {
            type : Array,
            default : []
        },
        createdBy : {
            type : String,
            default : "SYSTEM"
        },
        updatedBy : {
            type : String,
            default : "SYSTEM"
        },
        learningResources : {
            type : Array,
            default : []
        },
        isReusable : {
            type : Boolean,
            default : false,
            index : true
        },
        entityType : {
            type : String
        },
        taskSequence : {
            type : Array,
            default : []
        },
        taskCreationForm : {    
            type : String
        },
        metaInformation : {
            type : Object,
            default : {}
        },
        parentTemplateId : {
            type : "ObjectId",
            index: true
        },
        solutionId : "ObjectId",
        solutionExternalId : String,
        programId : "ObjectId",
        programExternalId : String,
        averageRating : {
            type : Number,
            default : 0
        },
        noOfRatings : {
            type : Number,
            default : 0
        },
        ratings : {
            type : Object,
            default : {
                1 : 0,
                2 : 0,
                3 : 0,
                4 : 0,
                5 : 0
            }
        }
    }
};