## Migrations

#### Steps to run the script files

This script is intended to address a particular bug in Diksha Prod, and it is unrelated to the 6.0.0 release.

Here is the ticket for this issue:- (Click Here)[https://project-sunbird.atlassian.net/browse/ED-3101].

In order to execute this migration script, we need to first log in to the pod where the service is running and then proceed with the provided instructions.

This script will update projects in cases where the user profile is missing and there is a mismatch between userProfile and userRoleInformations.

### Step 1:

    Navigate to /opt/projects/migrations/updateUserProfileAndMissMatchOfRoleInformation/

### Step 2:

Run the script to update projects.

    node updateProjectWithProfileData.js

### Setp 3: Validations 

upon executing this script we can validate the data using DBfind api 

#### check for userRoleInformation missing in projects 

Open the `updatedProjectIds.json` file and extract the project IDs from `userRoleInformationMissingProjectIds`. Utilize these project IDs in the following DBfind API to retrieve the corresponding projects.

{
    "query": {
        "_id": {
            "$in": [
                // Add project ids here
            ]
        }
    },
    "mongoIdKeys": [
        "_id"
    ],
    "projection": [
        "userRoleInformation"
    ],
    "limit": 200,
    "skip": 0
}

After obtaining the response, validate that all projects include userRoleInformation entries.

#### check for userProfile missing in projects 

Open the `updatedProjectIds.json` file and extract the project IDs from `userProfileMissingProjectIds`. Utilize these project IDs in the following DBfind API to retrieve the corresponding projects.

{
    "query": {
        "_id": {
            "$in": [
                // Add project ids here
            ]
        }
    },
    "mongoIdKeys": [
        "_id"
    ],
    "projection": [
        "userProfile"
    ],
    "limit": 200,
    "skip": 0
}

After obtaining the response, validate that all projects include userProfile entries.

#### check for userRoleInformation and userProfile missing in projects 

Open the `updatedProjectIds.json` file and extract the project IDs from `bothDataMissingProjectIds`. Utilize these project IDs in the following DBfind API to retrieve the corresponding projects.

{
    "query": {
        "_id": {
            "$in": [
                // Add project ids here
            ]
        }
    },
    "mongoIdKeys": [
        "_id"
    ],
    "projection": [
        "userRoleInformation","userProfile"
    ],
    "limit": 200,
    "skip": 0
}

After obtaining the response, validate that all projects include both userRoleInformation and userProfile entries.