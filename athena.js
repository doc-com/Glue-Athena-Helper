const AWS = require('aws-sdk');
const athena = new AWS.Athena({
	region: 'us-east-1'
});
function parseColumns(columns) {
	let output = '';
	columns.forEach((column, i) => {
		output += `\`${column.Name}\` ${column.Type}`
		if(i<(columns.length-1)) output+=','
	});
	return output;
}

function buildQuery(database, table, columns, bucket) {
	let cols = parseColumns(columns);
	let query = `CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.${table} (${cols})` +
	  ` ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'` +
	  ` WITH SERDEPROPERTIES (`+
		`'serialization.format' = '1'`+
	  `) LOCATION '${bucket}${table}/'`+
	  ` TBLPROPERTIES ('has_encrypted_data'='false');`;
	return query;
}

function createTable(database, table, columns, bucket) {
	let query = buildQuery(database, table, columns, bucket);
	athena.startQueryExecution({
		QueryString: query,
		QueryExecutionContext: {
			Database: database
		},
		ResultConfiguration: {
			OutputLocation: process.env.S3_OUTPUT_QUERY_LOCATION
		}
	}, (err, data) => {
		console.log('done ', table);
		if (err) console.log(err, err.stack);
  		else console.log(data);
	});
}

module.exports = {createTable, buildQuery, parseColumns};