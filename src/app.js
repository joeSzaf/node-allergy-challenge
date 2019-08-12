// Set up JDBC connection
var presto = require('presto-client')
var client = new presto.Client({user: 'presto', host: '34.74.56.14', catalog: 'hive', schema: 'leap'})

// sample allergy data
// {"resourceType":"AllergyIntolerance","id":"6ea84b5b-958e-46e4-9bc1-b433759e054d","onset":"2018-07-28T01:24:50+00:00","patient":{"reference":"urn:uuid:8f26f086-222e-44eb-936b-8dae1171182c"},"substance":{"coding":[{"system":"http://snomed.info/sct","code":"419474003","display":"Allergy to mould"}],"text":"Allergy to mould"},"status":"active","criticality":"CRITL","type":"allergy","category":"food","meta":{}}

// hard coded query
// var query = "SELECT " +
//               "json_extract_scalar(a.json,'$.substance.coding[0].code'), " +
//               "json_extract_scalar(a.json,'$.substance.coding[0].display') FROM allergyintolerance a " +
//               "WHERE " +
//               "json_extract_scalar(a.json,'$.patient.reference') = 'urn:uuid:75e27c31-0174-4987-ba61-39422d381905'"

// var query = "SELECT " +
//               "json_extract_scalar(json, '$.id'), " +
//               "json_extract_scalar(json,'$.name[0].family[0]'), " +
//               "json_extract_scalar(json,'$.name[0].given[0]') from patient p " +
//               "WHERE json_extract_scalar(json,'$.id') = '8f26f086-222e-44eb-936b-8dae1171182c'"

// Construct SQL query
var query = "SELECT " +
              "json_extract_scalar(a.json,'$.substance.coding[0].code'), " +
              "json_extract_scalar(a.json,'$.substance.coding[0].display') FROM allergyintolerance a " +
              "JOIN patient p " +
              "ON " +
              "json_extract_scalar(a.json,'$.patient.reference') = " +
              "CONCAT('urn:uuid:', json_extract_scalar(p.json,'$.id')) " +
              // "WHERE json_extract_scalar(p.json,'$.name[0].family[0]')  = 'Zboncak558' " +
              // "AND json_extract_scalar(p.json,'$.name[0].given[0]') = 'Marshall526'"
              "WHERE json_extract_scalar(p.json,'$.name[0].family[0]')  = 'Rascón630' " +
              "AND json_extract_scalar(p.json,'$.name[0].given[0]') = 'Eduardo902'"

// #####################


// 75e27c31-0174-4987-ba61-39422d381905

// var query = "SELECT " +
//               "json_extract_scalar(json, '$.id'), " +
//               "json_extract_scalar(json,'$.name[0].family[0]'), " +
//               "json_extract_scalar(json,'$.name[0].given[0]') from patient p " +
//               "WHERE json_extract_scalar(json,'$.name[0].family[0]')  = 'Zboncak558' " +
//               "AND json_extract_scalar(json,'$.name[0].given[0]') = 'Marshall526'"




              // "json_extract_scalar(json,'$.name[0].family[0]'), " +
              // "json_extract_scalar(json,'$.name[0].given[0]') from patient p " +
              // "WHERE json_extract_scalar(json,'$.name[0].family[0]')  = 'Zboncak558' " +
              // "AND json_extract_scalar(json,'$.name[0].given[0]') = 'Marshall526'"
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
