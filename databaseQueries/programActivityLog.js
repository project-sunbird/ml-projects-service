/**
 * ProgramActivityLog
 * @class
 */

module.exports = class ProgramActivityLog {
	/**
	 * Lists of ProgramActivityLog.
	 * @method
	 * @name find
	 * @param {Array} [filterData = "all"] - ProgramActivityLog filter query.
	 * @param {Array} [fieldsArray = "all"] - projected fields.
	 * @param {Array} [skipFields = "none"] - field not to include
	 * @returns {Array} Lists of ProgramActivityLog Documents.
	 */
	static find(filterData = 'all', fieldsArray = 'all', skipFields = 'none') {
		return new Promise(async (resolve, reject) => {
			try {
				let queryObject = filterData != 'all' ? filterData : {}
				let projection = {}

				if (fieldsArray != 'all') {
					fieldsArray.forEach((field) => {
						projection[field] = 1
					})
				}

				if (skipFields !== 'none') {
					skipFields.forEach((field) => {
						projection[field] = 0
					})
				}
				let programActivityLog = await database.models.programActivityLog.find(queryObject, projection).lean()
				return resolve(programActivityLog)
			} catch (error) {
				return reject(error)
			}
		})
	}

	/**
	 * Update ProgramActivityLog Documents.
	 * @method
	 * @name update
	 * @param {Object} [filterData] - filtered Query.
	 * @param {Object} [updateData] - update data.
	 * @returns {Array} - ProgramActivityLog Documents.
	 */
	static update(filterData = 'all', updateData, options = {}) {
		return new Promise(async (resolve, reject) => {
			try {
				let queryObject = filterData != 'all' ? filterData : {}
				let updatedData = await database.models.programActivityLog
					.findOneAndUpdate(queryObject, updateData, options)
					.lean()

				return resolve(updatedData)
			} catch (error) {
				return reject(error)
			}
		})
	}
}
