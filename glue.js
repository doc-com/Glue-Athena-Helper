const AWS = require('aws-sdk');
const glue = new AWS.Glue({
	region: 'us-east-1'
});

function parseColumns(columns) {
	let output = '';
	columns.forEach((column, i) => {
		output += `("${column.Name}", "${column.Type}", "${column.Name}", "${column.Type}")`
		if(i<(columns.length-1)) output+=','
	});
	return output;
}

function buildJobScript(database, tables, bucket, tableNameTransformation) {
	let script = '';
	tables.forEach((table, i) => {
		let columns = parseColumns(table.StorageDescriptor.Columns);
		let bucketPath = tableNameTransformation?tableNameTransformation(table.Name):table.Name;
		script +=
		`datasource${i} = glueContext.create_dynamic_frame.from_catalog(database = "${database}", table_name = "${table.Name}", transformation_ctx = "datasource${i}")\n`+
		`applymapping1 = ApplyMapping.apply(frame = datasource${i}, mappings = [${columns}], transformation_ctx = "applymapping1")\n`+
		`resolvechoice2 = ResolveChoice.apply(frame = applymapping1, choice = "make_struct", transformation_ctx = "resolvechoice2")\n`+
		`dropnullfields3 = DropNullFields.apply(frame = resolvechoice2, transformation_ctx = "dropnullfields3")\n`+
		`datasink4 = glueContext.write_dynamic_frame.from_options(frame = dropnullfields3, connection_type = "s3", connection_options = {"path": "${bucket}${bucketPath}/"}, format = "parquet", transformation_ctx = "datasink4")\n`+
		`\n`;
	});
	return script;
}

function getTables(database, owner, cb) {
	glue.getTables({
		DatabaseName: database
	},(err, data) => {
		if(err) return cb(err);
		let tables = data.TableList.filter(table => table.Owner === owner);
		cb(null, tables);
	})
}

module.exports = {getTables, parseColumns, buildJobScript};