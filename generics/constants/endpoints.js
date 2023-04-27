/**
 * name : api-responses.js
 * author : Aman Karki
 * Date : 16-July-2020
 * Description : All api response messages.
 */

module.exports = {
    GET_IMPROVEMENT_PROJECTS : "/v1/template/getImprovementProjects", // Unnati service
    DOWNLOADABLE_GCP_URL : "/v1/cloud-services/gcp/getDownloadableUrl", // Kendra service
    DOWNLOADABLE_AWS_URL : "/v1/cloud-services/aws/getDownloadableUrl", // Kendra service
    DOWNLOADABLE_AZURE_URL : "/v1/cloud-services/azure/getDownloadableUrl", // Kendra service
    VERIFY_TOKEN: "/v1/token/verify", // Call to sunbird service 
    SOLUTION_EXTERNAL_IDS_TO_INTERNAL_IDS : "/v1/solutions/externalIdsToInternalIds",
    LIST_SOLUTIONS : "/v1/solutions/list",
    LIST_USER_ROLES : "/v1/user-roles/list",
    LIST_ENTITY_TYPES : "/v1/entity-types/list",
    CREATE_PROGRAM_AND_SOLUTION : "/v1/users/createProgramAndSolution",
    LIST_ENTITIES : "/v1/entities/listByIds",
    USER_EXTENSION_GET_PROFILE : "/v2/user-extension/getProfile",
    USER_EXTENSION_UPDATE_USER_PROFILE : "/v1/user-extension/update",
    USER_PRIVATE_PROGRAMS : "/v1/users/privatePrograms",
    UPDATE_SOLUTIONS : "/v1/solutions/updateSolutions",
    LIST_PROGRAMS : "/v1/programs/list",
    PRESIGNED_GCP_URL : "/v1/cloud-services/gcp/preSignedUrls", // Kendra service
    PRESIGNED_AWS_URL : "/v1/cloud-services/aws/preSignedUrls", // Kendra service
    PRESIGNED_AZURE_URL : "/v1/cloud-services/azure/preSignedUrls", // Kendra service,
    VIEW_PROJECT_REPORT : "/v1/improvement-project/viewProjectReport", // dhiti service
    ENTITY_REPORT : "/v1/improvement-project/entityReport", // dhiti service
    ASSESSMENTS_CREATE : "/v1/assessments/create", // Should be kendra as api is not build checked for assessment
    ADD_ENTITY_TO_OBSERVATIONS : "/v1/observations/addEntityToObservation",
    ADD_ENTITIES_TO_SOLUTIONS : "/v1/solutions/addEntities",
    UPDATE_OBSERVATION : "/v1/observations/update",
    LIST_OBSERVATIONS : "/v1/observations/list",
    CREATE_ENTITY_ASSESSORS : "/v1/entityAssessors/create",
    CREATE_OBSERVATIONS : "/v1/observations/create",
    DETAILS_FORM : "/v1/forms/details",
    GET_USERS_BY_ENTITY_AND_ROLE : "/v1/entities/getUsersByEntityAndRole", //Kendra service
    LIST_PROGRAMS_BY_IDS : "/v1/programs/listByIds",
    REMOVE_SOLUTIONS_FROM_PROGRAM : "/v1/programs/removeSolutions",
    REMOVE_ENTITY_FROM_SOLUTION : "/v1/solutions/removeEntities",
    SOLUTION_BASED_ON_ROLE_LOCATION : "/v1/solutions/forUserRoleAndLocation",
    SOLUTION_DETAILS_BASED_ON_ROLE_LOCATION : "/v1/solutions/detailsBasedOnRoleAndLocation",
    LIST_ENTITIES_BY_LOCATION_IDS : "/v1/entities/listByLocationIds",
    CREATE_IMPROVEMENT_PROJECT_SOLUTION : "/v1/solutions/create",
    PROJECT_AND_TASK_REPORT : "/v1/improvement-project/projectAndTaskReport",
    FILES_DOWNLOADABLE_URL: "/v1/cloud-services/files/getDownloadableUrl",
    OBSERVATION_DETAILS : "/v1/observations/details",
    USER_READ_V5 : "/v5/user/read",
    GET_LOCATION_DATA : "/v1/location/search",
    CERTIFICATE_CREATE : "/api/v1/ProjectCertificate",
    PROJECT_CERTIFICATE_API_CALLBACK : "/v1/userProjects/certificateCallback",
    USER_READ_PRIVATE : "/private/user/v1/read", // !Caution: End point for reading user details without token. Do not use for public work flow
    GET_CERTIFICATE_KID : "/api/v1/PublicKey/search",
    PROGRAM_JOIN: "/v1/programs/join",
    IS_TARGETED_BASED_ON_USER_PROFILE : "/v1/solutions/isTargetedBasedOnUserProfile",
};
