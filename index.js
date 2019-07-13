const AWS = require('aws-sdk');
const glue = new AWS.Glue({
	region: 'us-east-1'
});
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

function buildQuery(database, table, columns) {
	let cols = parseColumns(columns);
	let query = `CREATE EXTERNAL TABLE IF NOT EXISTS ${database}.${table} (${cols})` +
	  ` ROW FORMAT SERDE 'org.apache.hadoop.hive.ql.io.parquet.serde.ParquetHiveSerDe'` +
	  ` WITH SERDEPROPERTIES (`+
		`'serialization.format' = '1'`+
	  `) LOCATION '${process.env.PARQUET_BUCKET}/${table}/'`+
	  ` TBLPROPERTIES ('has_encrypted_data'='false');`;
	return query;
}

function createTable(database, table, columns) {
	let query = buildQuery(table, columns);
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
let database = process.env.DATABASE_NAME;
glue.getTables({
	DatabaseName: database
},(err, data) => {
	data.TableList.forEach(table => {
		if(table.Owner === 'owner') {
			let name = table.Name.substring(15);
			createTable(database, name, table.StorageDescriptor.Columns);
		}
	});
})

