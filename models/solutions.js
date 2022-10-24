/**
 * name : solutions-model.js.
 * author : Vishnu.
 * created-date : 26-jan-2022.
 * Description : Schema for solutions.
 */
module.exports = {
    name: "solutions",
    schema: {
      externalId: String,
      isReusable: Boolean,
      name: String,
      description: String,
      author: String,
      parentSolutionId: "ObjectId",
      resourceType: Array,
      language: Array,
      keywords: Array,
      concepts: Array,
      scoringSystem: String,
      levelToScoreMapping: Object,
      themes: Array,
      flattenedThemes : Array,
      questionSequenceByEcm: Object,
      entityType: String,
      type: String,
      subType: String,
      entities: Array,
      programId: "ObjectId",
      programExternalId: String,
      programName: String,
      programDescription: String,
      entityProfileFieldsPerEntityTypes: Object,
      startDate: Date,
      endDate: {
        type : Date,
        index : true
      },
      status: String,
      evidenceMethods: Object,
      sections: Object,
      registry: Array,
      frameworkId: "ObjectId",
      frameworkExternalId: String,
      parentSolutionId: "ObjectId",
      noOfRatingLevels: Number,
      isRubricDriven: { type : Boolean, default: false },
      enableQuestionReadOut: { type : Boolean, default: false },
      isReusable: Boolean,
      roles: Object,
      observationMetaFormKey: String,
      updatedBy: String,
      captureGpsLocationAtQuestionLevel:{ type : Boolean, default: false },
      sendSubmissionRatingEmailsTo: String,
      creator: String,
      linkTitle: String,
      linkUrl: String,
      isAPrivateProgram : {
        default : false,
        type : Boolean
      },
      assessmentMetaFormKey : String,
      allowMultipleAssessemts : {
        default : false,
        type : Boolean
      },
      isDeleted: {
          default : false,
          type : Boolean,
          index : true
      },
      project : Object,
      referenceFrom : String,
      scope : {
        entityType : String,
        entities : {
          type : Array,
          index : true
        },
        roles : [{
          _id : "ObjectId",
          code : {
            type : String,
            index : true
          }
        }]
      },
      pageHeading: {
        default : "Domains",
        type : String
      },
      criteriaLevelReport : Boolean,
      license:Object,
      link: String,
      minNoOfSubmissionsRequired: {
        type: Number,
        default: 1
      },
      reportInformation : Object
    }
  };