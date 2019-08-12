// Set up JDBC connection
var presto = require('presto-client')
var client = new presto.Client({user: 'presto', host: '34.74.56.14', catalog: 'hive', schema: 'leap'})

// sample allergy data
// {"resourceType":"AllergyIntolerance","id":"6ea84b5b-958e-46e4-9bc1-b433759e054d","onset":"2018-07-28T01:24:50+00:00","patient":{"reference":"urn:uuid:8f26f086-222e-44eb-936b-8dae1171182c"},"substance":{"coding":[{"system":"http://snomed.info/sct","code":"419474003","display":"Allergy to mould"}],"text":"Allergy to mould"},"status":"active","criticality":"CRITL","type":"allergy","category":"food","meta":{}}

// hard coded query based on id of patient
// var query = "SELECT " +
//               "json_extract_scalar(a.json,'$.substance.coding[0].code'), " +
//               "json_extract_scalar(a.json,'$.substance.coding[0].display') FROM allergyintolerance a " +
//               "WHERE " +
//               "json_extract_scalar(a.json,'$.patient.reference') = 'urn:uuid:75e27c31-0174-4987-ba61-39422d381905'"

// Construct SQL query
var query = "SELECT " +
              "json_extract_scalar(a.json,'$.substance.coding[0].code'), " +
              "json_extract_scalar(a.json,'$.substance.coding[0].display') FROM allergyintolerance a " +
              "JOIN patient p " +
              "ON " +
              "json_extract_scalar(a.json,'$.patient.reference') = " +
              "CONCAT('urn:uuid:', json_extract_scalar(p.json,'$.id')) " +
              "WHERE json_extract_scalar(p.json,'$.name[0].family[0]')  = 'Zboncak558' " +
              "AND json_extract_scalar(p.json,'$.name[0].given[0]') = 'Marshall526'"

// Query the Analytics Engine
client.execute({
    query:   query,
    catalog: 'hive',
    schema:  'leap',
    state:   function(error, query_id, stats){
      // console.log({message:"status changed", id:query_id, stats:stats})
    },
    columns: function(error, data){
       // console.log({resultColumns: data})
     },
    data:    function(error, data, columns, stats){
      console.log(`Marshall526 Zboncak558 has the following ${data.length} allergies:`)
      data.forEach(item => {
        console.log(`  *${item[1]} (code: ${item[0]})`)
      })
    },
    success: function(error, stats){},
    error:   function(error){
      // console.log('e', error)
    }
})
