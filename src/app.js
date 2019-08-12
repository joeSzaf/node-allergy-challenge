// Set up JDBC connection
var presto = require('presto-client');
var client = new presto.Client({user: 'presto', host: '34.74.56.14', catalog: 'hive', schema: 'leap'});

// Construct SQL query
var query = "select json_extract_scalar(json,'$.name[0].family[0]'), " +
            "json_extract_scalar(json,'$.name[0].given[0]') from patient p " +
            "WHERE json_extract_scalar(json,'$.name[0].family[0]')  = 'Schuppe920' " +
            "AND json_extract_scalar(json,'$.name[0].given[0]') = 'Jean712'";

// Query the Analytics Engine
client.execute({
    query:   query,
    catalog: 'hive',
    schema:  'leap',
    state:   function(error, query_id, stats){ console.log({message:"status changed", id:query_id, stats:stats}); },
    columns: function(error, data){ console.log({resultColumns: data}); },
    data:    function(error, data, columns, stats){ console.log(data); },
    success: function(error, stats){},
    error:   function(error){ console.log('e', error) }
});
