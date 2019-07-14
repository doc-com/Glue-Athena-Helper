let glue = require('./glue');
let athena = require('./athena');

process.env.S3_OUTPUT_QUERY_LOCATION = '';
let database='';
let owner = '';
let bucket = '';
let tableNameTransformation = (name) => {

};


glue.getTables(database, owner, (err, tables) => {
	let script = glue.buildJobScript(database, tables, bucket, tableNameTransformation);
	console.log(script);
});

glue.getTables(database, owner, (err, tables) => {
	let i = 0;
	let int = setInterval(() => {
		if(i === tables.length){
			clearInterval(int);
			return;
		}
		let table = tables[i];
		let name = table.Name.substring(10);
		athena.createTable(database, name, table.StorageDescriptor.Columns, bucket);
		i++;
	},1000);
});