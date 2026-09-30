//
// This example shows how to use the library in traditional Promise style.
//

var Snowflake = require('snowflake-promise').Snowflake;

function main() {
  var snowflake = new Snowflake({
    account: '<account name>',
    username: '<username>',
    password: '<password>',
    database: 'SNOWFLAKE_SAMPLE_DATA',
    schema: 'TPCH_SF1',
    warehouse: 'DEMO_WH'
  });

  snowflake.connect()
    .then(function() {
      return snowflake.execute(
        'SELECT COUNT(*) FROM CUSTOMER WHERE C_MKTSEGMENT=:1',
        ['AUTOMOBILE']
      );
    })
    .then(function(rows) {
      console.log(rows);
    })
    .catch(function(err) {
      console.error(err);
    })
  ;
}

main();
