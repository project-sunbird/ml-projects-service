const mongoose = require('mongoose')
const { Schema } = mongoose

module.exports = {
	name: 'programActivityLog',

	schema: {
		date: {
			type: String, // YYYY-MM-DD (UTC)
			required: true,
			index: true,
		},

		activity: {
			improvementProject: {
				type: [
					{
						programId: {
							type: Schema.Types.ObjectId,
							required: true,
						},
						solutionIds: {
							type: [Schema.Types.ObjectId],
							default: [],
						},
					},
				],
				default: [],
			},
		},

		improvementProjectStatus: {},
	},

	compoundIndex: [
		{
			name: { date: 1, 'activity.improvementProject.programId': 1 },
			indexType: { unique: true },
		},
	],
}
