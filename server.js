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

// define the route
app.get('/', (req, res) => {
    if (Object.keys(req.query).length === 0)
        res.json({ "message": "Welcome to Epidemics API. For information on how to use, visit: https://github.com/gsalibi/epidemics-api" });
    else {
        let minLat = req.query.minLat == undefined ? 'latitude' : parseFloat(req.query.minLat);
        let maxLat = req.query.maxLat == undefined ? 'latitude' : parseFloat(req.query.maxLat);
        let minLon = req.query.minLon == undefined ? 'longitude' : parseFloat(req.query.minLon);
        let maxLon = req.query.maxLon == undefined ? 'longitude' : parseFloat(req.query.maxLon);
        let diseaseID = req.query.diseaseID == undefined ? 'DiseaseID' : parseInt(req.query.diseaseID);
        let startDate = req.query.startDate == undefined ? 0 : req.query.startDate;
        let endDate = req.query.endDate == undefined ? 0 : req.query.endDate;
        let city = req.query.city == undefined ? '%' : req.query.city;
        let disease = req.query.disease == undefined ? '%' : req.query.disease;
    
        execSQLQuery("\
        SELECT Diseases.Name as disease, Outbreaks.NumberOfCases, Outbreaks.FatalCases, Cities.name as city, States.name as state, \
        Countries.name_pt as country, Cities.latitude as latitude, Cities.longitude as longitude, DATE_FORMAT(Date, '%Y-%m-%d') as date \
        FROM Outbreaks INNER JOIN Cities  \
            ON Outbreaks.CityID = Cities.cityID \
        INNER JOIN States \
            ON Cities.stateID = States.stateID \
        INNER JOIN Countries \
            ON States.CountryID = Countries.id \
        INNER JOIN Diseases  \
            ON Outbreaks.DiseaseID = Diseases.idDisease \
        WHERE DiseaseID = " + diseaseID + " and \
        Date >= IF(" + startDate + "= 0, LastUpdate, '" + startDate + "') and \
        Date <= IF(" + endDate + "= 0, LastUpdate, '" + endDate + "') and \
        latitude >= " + minLat + " and \
        latitude <= " + maxLat + " and \
        longitude >= " + minLon + " and \
        longitude <= " + maxLon + " and \
        (Cities.name LIKE '" + city + "' or REPLACE(Cities.name, ' ', '')  = '" + city + "') and \
        (Diseases.name LIKE '" + disease + "' or REPLACE(Cities.name, ' ', '')  = '" + disease + "')" , res);
    }
});


// define another route
app.get('/test/:id?', (req, res) => {
    let filter = '';
    if(req.params.id) filter = ' WHERE id=' + parseInt(req.params.id);
    execSQLQuery('SELECT * FROM TestTable' + filter, res);
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
          //res.json("Consulta invalida");
          res.json(error);
        else
          res.json(results);
        connection.end();
        console.log('Resultado exibido!');
    });
  }
