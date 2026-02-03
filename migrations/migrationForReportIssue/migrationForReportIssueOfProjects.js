    /**
     * fetchPrivateProgramData.js
     *
     * Usage:
     *   node fetchPrivateProgramData.js <programId>
     *
     */
    /**
     *  node command sample:- node migrations/migrationForReportIssue/migrationForReportIssueOfProjects.js 680893ff3d8d030008cd037a --update=true  --baseUrl=<---baseUrl---> --token=<---token--->     * 
     */

    const path = require("path");
    const fs = require("fs");
    const { MongoClient, ObjectId } = require("mongodb");
    const _ = require('lodash');
    const {updateTasksUsingPublicProject, criteriaValidation, updateCorruptedProjectsInDB, reIssueCertificates} = require("./eligibilityAndTasksValidator")
    require("dotenv").config({ path: path.join(__dirname, "../../") + "/.env" });
    const mongo_url = process.env.MONGODB_URL;
    if (!mongo_url) {
      console.error("❌ MONGODB_URL not set");
      process.exit(1);
    }
    let doUpdate = false;
    const programIdArg = process.argv[2];
    const batchSize = 100;
    const doUpdateArg = process.argv.find(arg => arg.startsWith('--update='));
    doUpdate = doUpdateArg ? doUpdateArg.split('=')[1] : null;
    doUpdate = doUpdate == "true" ? true : false
    
      
    const tokenArg = process.argv.find(arg => arg.startsWith('--token='));
    const userToken = tokenArg ? tokenArg.split('=')[1] : null;

    if (!programIdArg || !ObjectId.isValid(programIdArg)) {
      console.error("❌ Please provide a valid programId");
      process.exit(1);
    }

    if(!userToken){
      console.log("--token arg is required");
      process.exit(1);
    }

    // get programId from command line argument
    const programId = new ObjectId(programIdArg);
    const db_name = mongo_url.split("/").pop();
    const url = mongo_url.split(db_name)[0];

    /* -------------------- OUTPUT SETUP -------------------- */
    const timestamp = new Date().toISOString().replace(/[:.]/g, "-");
    const output_dir = path.join(__dirname, "output");
    const masterFilePath = path.join(output_dir, `${programId.toString()}-${timestamp}.json`);
    let masterJsonData;
    masterJsonData = doUpdate ? {mode : "WRITE-MODE"} : {mode : "READ-MODE"};

    if (!fs.existsSync(output_dir)) {
      fs.mkdirSync(output_dir, { recursive: true });
    }
    fs.writeFileSync(
      masterFilePath,
      JSON.stringify(masterJsonData, null, 2),
      "utf8"
    );      

    const sleep = (ms) => new Promise(resolve => setTimeout(resolve, ms));

    /* -------------------- MAIN EXECUTION -------------------- */
    (async () => {
    let connection;
    try {
      connection = await MongoClient.connect(url, {
        useNewUrlParser: true,
        useUnifiedTopology: true
      });
      const db = connection.db(db_name);
      const priorityMap = {
        "submitted" : 3,
        "inprogress" : 2,
        "started" : 1
      }

      // fetch program details
      const program = await db.collection("programs").findOne(
        { _id: programId, isAPrivateProgram : false },
        { projection: { components: 1, name: 1 } }
      );
      // console.log("program ", program);
      if (!program) {
        console.error("❌ Program not found:", programId.toHexString());
        process.exit(1);
      }
      const components = program.components || [];
      console.log(`✅ Program found. Components count: ${components.length}`);

      const output = {
        programId: programId.toHexString(),
        programName: program.name || "",
        generatedAt: new Date().toISOString(),
        components: {}
      };
      let privateUserIdsSet =[]
      /* ---------- 2. PROCESS EACH COMPONENT ---------- */

      for (const componentId of components) {
        const componentKey = componentId.toHexString();
        /* ---- Fetch private solutions ---- */
        const privateSolutions = await db
          .collection("solutions")
          .find(
          {
              isAPrivateProgram: true,
              parentSolutionId: componentId,
              type: "improvementProject"
          },
          {
              projection: {
              _id: 1,
              parentSolutionId: 1,
              programId: 1,
              isAPrivateProgram: 1
              }
          }
        ).toArray();

        const enrichedSolutions = [];
        /* ---- Fetch projects for each solution ---- */
        for (const solution of privateSolutions) {
          const projects = await db
          .collection("projects")
          .find(
              {
              solutionId: solution._id,
              isAPrivateProgram: true
              },
              {
              projection: {
                  _id: 1,
                  title: 1,
                  status: 1,
                  userId: 1,
                  "certificate.eligible": 1,
                  programId: 1,
                  solutionId: 1,
                  isAPrivateProgram: 1
              }
              }
          ).toArray();

          enrichedSolutions.push({
          _id: solution._id.toHexString(),
          parentSolutionId: solution.parentSolutionId
              ? solution.parentSolutionId.toHexString()
              : null,
          programId: solution.programId
              ? solution.programId.toHexString()
              : null,
          isAPrivateProgram: solution.isAPrivateProgram,
          projects: projects.map((p) => ({
              _id: p._id.toHexString(),
              title: p.title,
              status: p.status,
              userId: p.userId,
              certificateEligible:
              p.certificate && p.certificate.eligible === true
          }))
          });

          projects.forEach(p => {
              if (p.userId) {
              privateUserIdsSet.push(p.userId);
              }
          });
        }

        output.components[componentKey] = {
            privateSolutions: enrichedSolutions
        };
      }
      /* ---------- 3. COMPONENT → USER → PRIVATE PROJECT AGGREGATION ---------- */
      let componentUserPrivateProjects = {};

      for (const [componentId, componentData] of Object.entries(output.components)) {
        const userMap = {};
        componentData.privateSolutions.forEach(solution => {
          solution.projects.forEach(project => {
            if (!project.userId) return;

            if (!userMap[project.userId]) {
              userMap[project.userId] = {
                userId: project.userId,
                privateProjectIds: []
              };
            }

            userMap[project.userId].privateProjectIds.push(project._id);
          });
        });

        // Only add component if at least one user has projects
        if (Object.keys(userMap).length > 0) {
          componentUserPrivateProjects[componentId] = Object.values(userMap);
        }
      }

      if (fs.existsSync(masterFilePath)) {
        const fileContent = fs.readFileSync(masterFilePath, "utf8");
        masterJsonData = fileContent ? JSON.parse(fileContent) : {};
      }

      masterJsonData["component_user_private_projects"] = componentUserPrivateProjects;

      fs.writeFileSync(
        masterFilePath,
        JSON.stringify(masterJsonData, null, 2),
        "utf8"
      );

      const finalResult = [];
      const skippedComponents = [];   // track skipped components

      for (const [componentId, users] of Object.entries(componentUserPrivateProjects)) {
          const componentSolution = await db.collection("solutions").findOne(
          {
            _id: new ObjectId(componentId),
            isAPrivateProgram: false
          },
          {
            projection: { scope: 1 }
          }
        );
        // 🔹 SKIP COMPONENT IF SOLUTION / SCOPE NOT PRESENT
        if (!componentSolution || !componentSolution.scope) {
          skippedComponents.push({
            componentId,
            reason: !componentSolution
              ? "Public component solution not found"
              : "Scope missing in public component solution"
          });
          continue; // ⛔ skip this component completely
        }
        for (const userEntry of users) {
          let targetedPrivateProjects = [];
          const { userId, privateProjectIds } = userEntry;
          // 1️⃣ Check public project
          const publicProject = await db.collection("projects").findOne({
            solutionId: new ObjectId(componentId),
            userId,
            isAPrivateProgram: false
          });

          // console.log("Public project for user:", userId, "is", publicProject ? "found" : "not found");
          if(!publicProject) {
            userEntry.privateProjectIds = [];
            continue;
          }

          // 2️⃣ Fetch private projects
          const privateProjects = await db.collection("projects").find({
            _id: { $in: privateProjectIds.map(id => new ObjectId(id)) },
            userId,
            isAPrivateProgram: true
          }).toArray();

          let validPrivateProjects = privateProjects.filter(project => priorityMap[project["status"].toLowerCase()] > priorityMap[publicProject["status"].toLowerCase()]);

          // Find the project with the highest priority
          let highestPriorityProject = null;
          let highestPriorityValue = -1;

          for (const project of validPrivateProjects) {
            if (!project || !project.status) continue;
          
            const statusKey = project.status.toLowerCase();
            const priority = priorityMap[statusKey];
          
            if (priority && priority > highestPriorityValue) {
              highestPriorityValue = priority;
              highestPriorityProject = project;
            }
          }
          
          // Store back as an array of single item
          validPrivateProjects = highestPriorityProject ? [highestPriorityProject] : [];
          if(validPrivateProjects.length == 0) {
            userEntry.privateProjectIds = [];
            continue;
          }

          const ignoredMissingRoleInfo = [];
          const evaluatedProjects = [];

          for (const project of validPrivateProjects) {
            if (!project.userRoleInformation) {
                ignoredMissingRoleInfo.push(project._id.toString());
                let generateuserRoleInfo = buildUserRoleInformationFromProfile(project.userProfile);
                if (generateuserRoleInfo) {
                  project.userRoleInformation = generateuserRoleInfo;
                } else {
                continue;
            }
            }
            // 3️⃣ Targeting check
            const targeted = isProjectTargeted(
              componentSolution,
              project.userRoleInformation
            );

            if(targeted){
              targetedPrivateProjects.push(project._id.toString());
            }

            evaluatedProjects.push({
              projectId: project._id.toString(),
              targeted,
              updatedAt: project.updatedAt,
              userRoleInformation: project.userRoleInformation,
              solutionScope: componentSolution.scope,
              status: project.status,
              certificateEligible: project.certificate && project.certificate.eligible === true
            });
          }

          finalResult.push({
            componentId,
            userId,
            ignoredPrivateProjectMissingUserRoleInformation: ignoredMissingRoleInfo,
            evaluatedPrivateProjects: evaluatedProjects
          });
          userEntry.privateProjectIds = targetedPrivateProjects;
        }
      }

      const summaryMap = {};

      for (const entry of finalResult) {
        const { componentId, evaluatedPrivateProjects } = entry;

        if (!summaryMap[componentId]) {
          summaryMap[componentId] = {
            componentId,
            projectsCreatedDueToBug: []
          };
        }

        const targetedProjects = evaluatedPrivateProjects.filter(
          p => p.targeted === true
        );

        if (targetedProjects.length > 0) {
          let projectToAdd = targetedProjects[0];

          if (targetedProjects.length > 1) {
            targetedProjects.forEach(p => {
              if (new Date(p.updatedAt) > new Date(projectToAdd.updatedAt)) {
                projectToAdd = p;
              }
            });
          }

          summaryMap[componentId].projectsCreatedDueToBug.push(projectToAdd.projectId);
        }
      }
      const summary = Object.values(summaryMap)
        .filter(c => c.projectsCreatedDueToBug.length > 0);
      const combinedOutput = {
        finalResult,
        summary
      };


    if(fs.existsSync(masterFilePath)){
      const fileContent = fs.readFileSync(masterFilePath, "utf8");
      masterJsonData = fileContent ? JSON.parse(fileContent) : {};
    }

    masterJsonData["private_project_bug_analysis"] = combinedOutput;

    fs.writeFileSync(
      masterFilePath,
      JSON.stringify(masterJsonData, null, 2),
      "utf8"
    );

    function buildUserRoleInformationFromProfile(userProfile) {
      if (!userProfile) return null;
      const roleInfo = {};
      /* -------- ROLE FROM profileUserTypes -------- */
      if (Array.isArray(userProfile.profileUserTypes)) {
        const roles = userProfile.profileUserTypes
          .map(r => r.subType ? r.subType : r.type) // prefer subType
          .filter(Boolean)
          .map(r => r.toUpperCase());

        if (roles.length > 0) {
          roleInfo.role = roles.join(","); // comma-separated roles
        }
      }

      /* -------- LOCATION FROM userLocations -------- */
      if (Array.isArray(userProfile.userLocations)) {
        userProfile.userLocations.forEach(loc => {
          if (!loc.type) return;

          // school → use code, others → use id
          if (loc.type === "school" && loc.code) {
            roleInfo[loc.type] = loc.code;
          } else if (loc.id) {
            roleInfo[loc.type] = loc.id;
          }
        });
      }
      return Object.keys(roleInfo).length ? roleInfo : null;
    }

    function isProjectTargeted(solution, userRoleInformation) {

      /* ---------------- HARD FALSE CHECKS ---------------- */

      if (!solution || !solution.scope) return false;
      if (!userRoleInformation || typeof userRoleInformation !== "object") return false;

      const { scope } = solution;
      // console.log("scop------------e", scope);

      if (
        !scope.entityType ||
        !Array.isArray(scope.entities) ||
        !Array.isArray(scope.roles)
      ) {
        return false;
      }

      /* ---------------- 1️⃣ ROLE CHECK ---------------- */

      // user role can be comma-separated
      const userRoles = userRoleInformation.role
        ? userRoleInformation.role.split(",").map(r => r.trim())
        : [];

      // always include ALL_ROLES
      userRoles.push("ALL");
    //   console.log("userRoles------------->", userRoles);
    //   const solutionRoles = scope.roles.map(r => r.code);
    //   console.log("solutionRoles------------->", solutionRoles);
    //   const roleMatched = userRoles.some(role =>
    //     solutionRoles.includes(role)
    //   );

    // normalize user roles
    const userRolesNormalized = userRoles.map(r => r.toLowerCase());

    // normalize solution roles
    const solutionRolesNormalized = scope.roles.map(r =>
      r.code.toLowerCase()
    );

    const roleMatched = userRolesNormalized.some(role =>
      solutionRolesNormalized.includes(role)
    );
    // console.log("roleMatched------------->", roleMatched);
      if (!roleMatched) return false;

      /* ---------------- 2️⃣ ENTITY CHECK ---------------- */

      // remove role & type → collect registryIds + entityTypes
      const registryIds = [];
      const entityTypes = [];

      Object.entries(userRoleInformation)
      .filter(([key]) => key !== "role" && key !== "type")
      .forEach(([key, value]) => {
        if (!value) return;
        registryIds.push(value);
        entityTypes.push(key);
      });


      if (!registryIds.length || !entityTypes.length) return false;

      // entityType match AND entityId match (ANY ONE is enough)
      const entityMatched =
        entityTypes.includes(scope.entityType) &&
        registryIds.some(id => scope.entities.includes(id));

      return entityMatched;
    }
    /* ---------- 3. WRITE OUTPUT ---------- */

    const output_path = path.join(
    output_dir,
     `program_private_project_data_${timestamp}.json`
    );

    if(fs.existsSync(masterFilePath)){
      const fileContent = fs.readFileSync(masterFilePath, "utf8");
      masterJsonData = fileContent ? JSON.parse(fileContent) : {};
    }

    masterJsonData["program_private_project_data"] = output;

    fs.writeFileSync(masterFilePath, JSON.stringify(masterJsonData, null, 2), "utf8");

    //-----------------------------------------------update the project
    let projectsToBeUpdated = summary;    
    const programsToBeDeleted = new Set();
    const solutionsToBeDeleted = new Set();
    let publicToPrivateProjectMap = {};
  for (const [componentId, users] of Object.entries(componentUserPrivateProjects)) {
    const solution = await db.collection("solutions").findOne(
      { _id: new ObjectId(componentId) },
      { _id: 1, externalId: 1 , name:1, programId: 1 , description: 1}
    );
    if (!solution) {
      print(`❌ Solution not found for componentId: ${componentId}`);
      continue;
    }
    /* Fetch program once */
    const program = await db.collection("programs").findOne(
      { _id: solution.programId },
      {
        projection: {
          _id: 1,
          externalId: 1,
          name: 1,
          description: 1
        }
      }
    );
    if (!program) {
      print(`❌ Program not found for solution: ${solution._id}`);
      continue;
    }

    let projectsPerComponent = [];

    for(const userEntry of users){
      const {userId, privateProjectIds} = userEntry;
      // if(["680894a73d8d030008cd0388", "685bd9ac3d8d030008fda1bb"].includes(componentId)) console.log(privateProjectIds)
      if(privateProjectIds.length == 0) continue;
      /* Iterate each bug project */
      const projectObjectIds = privateProjectIds.map(id => new ObjectId(id));
      let projects = await db.collection("projects").find(
        { _id: { $in: projectObjectIds } },
        { programId: 1, solutionId: 1, status: 1, certificate: 1, tasks : 1, attachments : 1 }
      ).toArray();
      projects.forEach(project => {
        if (project.programId) {
          programsToBeDeleted.add(project.programId.toString());
        }
  
        if (project.solutionId) {
          solutionsToBeDeleted.add(project.solutionId.toString());
        }
  
      });
      const publicProject = await db.collection("projects").findOne(
        {
          solutionId: new ObjectId(componentId),
          isAPrivateProgram: false,
          userId
        },
        {
          projection: {
            _id: 1,
            tasks: 1,
            attachments: 1
          }
        }
      );
      // store the private project for which a public project was deleted
      publicToPrivateProjectMap[publicProject._id.toString()] = "";

      // deleting public project before converting private project to public
      if(doUpdate){
        await db.collection("projects").deleteOne(
          {_id : publicProject._id}
        )
      }

      // update tasks.referenceId & tasks.externalId of every privateProject
      projects = await updateTasksUsingPublicProject(projects, publicProject);

      // validate criteria for each project
      for (let project of projects) {
        try {
          // store the private project for which a public project was deleted
          publicToPrivateProjectMap[publicProject._id.toString()] = project._id.toString();

          const validationResult = await criteriaValidation(project);
          project.eligible = validationResult && validationResult.success === true;
          projectsPerComponent.push(project);
        } catch (error) {
          project.eligible = false;      
          console.error(
            `❌ Criteria validation failed for project ${project._id.toString()}`,
            error.message || error
          );
        }
      }

    }
    const batches = _.chunk(projectsPerComponent, batchSize);

    for(const batch of batches){
      // update tasks & certificate.eligibility in DB
      await updateCorruptedProjectsInDB(batch, db, solution, program, doUpdate, masterFilePath);
  
      /* Perform updates ONLY if --update flag is passed */
      if (!doUpdate) {
        console.log("Dry run only. Skipping Certificate Reissue.");
        continue;
      }
  
      await reIssueCertificates(batch, userToken, masterFilePath);
    
      // ⏸ Pause for 30 seconds after certificates are re-issued
      console.log("⏳ Waiting for 30 seconds before processing next batch...");
      await sleep(30 * 1000); // 30 seconds
    }
  }

  /* 🧹 Delete programs and solutions ONLY if update mode */
  if (doUpdate) {
    /* Delete Programs */
    if (programsToBeDeleted.size > 0) {
      const programIds = Array.from(programsToBeDeleted).map(id => new ObjectId(id));

      console.log("🗑 Deleting Programs:", programIds);

      const programDeleteResult = await db.collection("programs").deleteMany({
        _id: { $in: programIds }, isAPrivateProgram: true
      });

      console.log(`✅ Deleted ${programDeleteResult.deletedCount} programs`);
    } else {
      console.log("ℹ️ No programs to delete");
    }

    /* Delete Solutions */
    if (solutionsToBeDeleted.size > 0) {
      const solutionIds = Array.from(solutionsToBeDeleted).map(id => new ObjectId(id));

      console.log("🗑 Deleting Solutions:", solutionIds);

      const solutionDeleteResult = await db.collection("solutions").deleteMany({
        _id: { $in: solutionIds }, isAPrivateProgram: true
      });

      console.log(`✅ Deleted ${solutionDeleteResult.deletedCount} solutions`);
    } else {
      console.log("ℹ️ No solutions to delete");
    }
  } else {
    console.log("ℹ️ Dry run mode. Skipping delete operations.");
  }



  const deletionLog = {
    timestamp: new Date().toISOString(),
    programsDeleted: Array.from(programsToBeDeleted),
    solutionsDeleted: Array.from(solutionsToBeDeleted)
  };

  if(fs.existsSync(masterFilePath)){
    const fileContent = fs.readFileSync(masterFilePath, "utf8");
    masterJsonData = fileContent ? JSON.parse(fileContent) : {};
  }

  masterJsonData["deleted_to_replacement_id_map"] = publicToPrivateProjectMap;

  fs.writeFileSync(
    masterFilePath,
    JSON.stringify(masterJsonData, null, 2),
    "utf8"
  );

  await connection.close();
  process.exit(0);
} catch (err) {
    console.error("❌ Fatal error:", err);
    if (connection) await connection.close();
    process.exit(1);
}
})();