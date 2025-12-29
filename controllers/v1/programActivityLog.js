/**
 * name : programActivityLog.js
 * author : prajwal
 * created-date : 23-Dec-2025
 * Description : Stores program & solution information of a project.
 */

module.exports = class ProgramActivityLog extends Abstract {
	constructor() {
		super('programActivityLog')
	}

	static get name() {
		return 'programActivityLog'
	}
}
