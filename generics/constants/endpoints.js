/**
 * name : api-responses.js
 * author : Aman Karki
 * Date : 16-July-2020
 * Description : All api response messages.
 */

module.exports = {
    GET_IMPROVEMENT_PROJECTS : "/api/v1/template/getImprovementProjects", // Unnati service
    DOWNLOADABLE_URL : "/api/v1/cloud-services/files/getDownloadableUrl", // Kendra service
    VERIFY_TOKEN: "/api/v1/token/verify", // Call to sunbird service 
    SOLUTION_EXTERNAL_IDS_TO_INTERNAL_IDS : "/api/v1/solutions/externalIdsToInternalIds",
    LIST_SOLUTIONS : "/api/v1/solutions/list",
    LIST_USER_ROLES : "/api/v1/user-roles/list",
    LIST_ENTITY_TYPES : "/api/v1/entity-types/list",
    CREATE_PROGRAM_AND_SOLUTION : "/api/v1/users/createProgramAndSolution",
    LIST_ENTITIES : "/api/v1/entities/listByIds",
    USER_EXTENSION_GET_PROFILE : "/api/v2/user-extension/getProfile",
    USER_EXTENSION_UPDATE_USER_PROFILE : "/api/v1/user-extension/update",
    USER_PRIVATE_PROGRAMS : "/api/v1/users/privatePrograms",
    UPDATE_SOLUTIONS : "/api/v1/solutions/updateSolutions",
    LIST_PROGRAMS : "/api/v1/programs/list",
    GET_USER_ORGANISATIONS : "/api/v1/users/getUserOrganisationsAndRootOrganisations",
    PRESIGNED_GCP_URL : "/api/v1/cloud-services/gcp/preSignedUrls", // Kendra service
    PRESIGNED_AWS_URL : "/api/v1/cloud-services/aws/preSignedUrls", // Kendra service
    PRESIGNED_AZURE_URL : "/api/v1/cloud-services/azure/preSignedUrls", // Kendra service,
    VIEW_PROJECT_REPORT : "/api/v1/improvement-project/viewProjectReport", // dhiti service
    ENTITY_REPORT : "/api/v1/improvement-project/entityReport", // dhiti service
    ASSESSMENTS_CREATE : "/api/v1/assessments/create", // Should be kendra as api is not build checked for assessment
    ADD_ENTITY_TO_OBSERVATIONS : "/api/v1/observations/addEntityToObservation",
    ADD_ENTITIES_TO_SOLUTIONS : "/api/v1/solutions/addEntities",
    UPDATE_OBSERVATION : "/api/v1/observations/update",
    LIST_OBSERVATIONS : "/api/v1/observations/list",
    CREATE_ENTITY_ASSESSORS : "/api/v1/entityAssessors/create",
    CREATE_OBSERVATIONS : "/api/v1/observations/create",
    DETAILS_FORM : "/api/v1/forms/details",
    GET_USERS_BY_ENTITY_AND_ROLE : "/api/v1/entities/getUsersByEntityAndRole", //Kendra service
    LIST_PROGRAMS_BY_IDS : "/api/v1/programs/listByIds",
    REMOVE_SOLUTIONS_FROM_PROGRAM : "/api/v1/programs/removeSolutions",
    REMOVE_ENTITY_FROM_SOLUTION : "/api/v1/solutions/removeEntities",
    SOLUTION_BASED_ON_ROLE_LOCATION : "/api/v1/solutions/forUserRoleAndLocation",
    SOLUTION_DETAILS_BASED_ON_ROLE_LOCATION : "/api/v1/solutions/detailsBasedOnRoleAndLocation",
    LIST_ENTITIES_BY_LOCATION_IDS : "/api/v1/entities/listByLocationIds",
    CREATE_IMPROVEMENT_PROJECT_SOLUTION : "/api/v1/solutions/create",
    PROJECT_AND_TASK_REPORT : "/api/v1/improvement-project/projectAndTaskReport",

};
