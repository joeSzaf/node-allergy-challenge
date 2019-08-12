// Set up JDBC connection
var presto = require('presto-client')
var client = new presto.Client({user: 'presto', host: '34.74.56.14', catalog: 'hive', schema: 'leap'})

// Construct SQL query
var query = "SELECT " +
              "json_extract_scalar(json,'$.name[0].family[0]'), " +
              "json_extract_scalar(json,'$.name[0].given[0]') from patient p " +
              "WHERE json_extract_scalar(json,'$.name[0].family[0]')  = 'Zboncak558' " +
              "AND json_extract_scalar(json,'$.name[0].given[0]') = 'Marshall526'";
              // "JOIN immunization i " +
              // "ON (split_part(json_extract_scalar(i.json, '$.patient.reference'), ':', 3) = json_extract_scalar(p.json, '$.id')) " +
              // "WHERE " +
              // "json_extract_scalar(p.json, '$.birthDate') > '2005-01-01' " +
              // "AND json_extract_scalar(p.json, '$.birthDate') < '2005-12-31' " +
              // "AND json_extract_scalar(p.json, '$.address[0].city') = 'Boston' " +
              // "AND (split_part(json_extract_scalar(p.json, '$.careProvider[0].reference'), ':', 2) = " +
              // "json_extract_scalar(dr.json, '$.id')) " +
              // "AND (json_extract_scalar(i.json, '$.vaccineCode.coding[0].code') = '62' " +
              // "OR json_extract_scalar(i.json, '$.vaccineCode.coding[0].code') = '114' " +
              // "OR json_extract_scalar(i.json, '$.vaccineCode.coding[0].code') = '115') " +
              // "ORDER BY json_extract_scalar(p.json, '$.name[0].family[0]'), " +
              // "json_extract_scalar(p.json, '$.name[0].given[0]') ";

let queriedData = []

// Query the Analytics Engine
client.execute({
    query:   query,
    catalog: 'hive',
    schema:  'leap',
    state:   function(error, query_id, stats){
      console.log({message:"status changed", id:query_id, stats:stats})
    },
    columns: function(error, data){
       console.log({resultColumns: data})
     },
    data:    function(error, data, columns, stats){
      queriedData = data
      console.log("queriedData:", queriedData)
    },
    success: function(error, stats){},
    error:   function(error){
      console.log('e', error)
    }
});
