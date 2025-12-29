const progActLogQueries = require(DB_QUERY_BASE_PATH + '/programActivityLog')

module.exports = class ProgramActivityLog {
	/**
	 * Creation of program activity log.
	 * @method
	 * @name addProgramActivityLog
	 * @param {String} programId - project id.
	 * @param {String} solutionId - solution id.
	 * @returns {Object} program activity log.
	 */
	static addProgramActivityLog(programId, solutionId) {
		return new Promise(async (resolve, reject) => {
			try {
				programId = UTILS.convertStringToObjectId(programId)
				solutionId = UTILS.convertStringToObjectId(solutionId)

				const date = new Date().toISOString().slice(0, 10) // gives date in YYYY-MM-DD format

				/**
				 * STEP 1: Ensure DATE document exists
				 */
				await progActLogQueries.update(
					{ date },
					{
						$setOnInsert: {
							date,
							activity: { improvementProject: [] },
						},
					},
					{ upsert: true }
				)

				/**
				 * STEP 2: Ensure PROGRAM exists for that date
				 */
				const programUpdate = await progActLogQueries.update(
					{
						date: date,
						'activity.improvementProject.programId': { $ne: programId },
					},
					{
						$push: {
							'activity.improvementProject': {
								programId,
								solutionIds: [],
							},
						},
					},
					{ new: false }
				)

				/**
				 * STEP 3: Add SOLUTION uniquely
				 */
				await progActLogQueries.update(
					{
						date: date,
						'activity.improvementProject.programId': programId,
					},
					{
						$addToSet: {
							'activity.improvementProject.$.solutionIds': solutionId,
						},
					},
					{ new: false }
				)

				return resolve({
					success: true,
					message: CONSTANTS.apiResponses.PROGRAM_ACTIVITY_LOG_UPDATED,
				})
			} catch (error) {
				return reject({
					status: error.status ? error.status : HTTP_STATUS_CODE.internal_server_error.status,
					success: false,
					message: error.message,
					data: [],
				})
			}
		})
	}
}
