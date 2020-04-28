require('dotenv').config({path:'.env'})
const express = require('express');
const app = express();   // create express app      
const bodyParser = require('body-parser');
const PORT = process.env.PORT || 3000
const mysql = require('mysql');


// parse requests of content-type - application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: true }))

// parse requests of content-type - application/json
app.use(bodyParser.json())

// define a simple route
app.get('/', (req, res) => {
    res.json({ "message": "Welcome to Epidemics API. For information on how to use, visit: https://github.com/gsalibi/epidemics-api" });
});


// define another route
app.get('/test/:id?', (req, res) => {
    let filter = '';
    if(req.params.id) filter = ' WHERE id=' + parseInt(req.params.id);
    execSQLQuery('SELECT * FROM TestTable' + filter, res);
});// define another route


app.get('/query', (req, res) => {
    execSQLQuery('SELECT * FROM Cities', res);
});

// listen for requests
app.listen(PORT, () => console.log(`Listening on ${ PORT }`))

function execSQLQuery(sqlQry, res){
    const connection = mysql.createConnection({
      host     : process.env.DB_HOST,
      port     : process.env.DB_PORT,
      user     : process.env.DB_USER,
      password : process.env.DB_PASS,
      database : process.env.DB_NAME
    });
  
    connection.query(sqlQry, function(error, results, fields){
        if(error) 
          res.json(error);
        else
          res.json(results);
        connection.end();
        console.log('Resultado exibido!');
    });
  }