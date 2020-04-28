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
    if (Object.keys(req.query).length === 0)
        res.json({ "message": "Welcome to Epidemics API. For information on how to use, visit: https://github.com/gsalibi/epidemics-api" });
    else {
        let minLat = req.query.minLat;
        let maxLat = req.query.maxLat;
        let minLon = req.query.minLon;
        let maxLon = req.query.maxLon;
        let diseaseID = req.query.diseaseID;
    
        parseInt(minLat);
    
        execSQLQuery("\
        SELECT Diseases.Name as disease, Outbreaks.NumberOfCases, Cities.name as city, States.name as state, \
        Countries.name_pt as country, Cities.latitude as latitude, Cities.longitude as longitude, Outbreaks.Date as date \
        FROM `Outbreaks` \
        INNER JOIN `Cities`  \
            ON Outbreaks.CityID = Cities.cityID \
        INNER JOIN `States` \
            ON Cities.stateID = States.stateID \
        INNER JOIN `Countries` \
            ON States.CountryID = Countries.id \
        INNER JOIN `Diseases`  \
            ON Outbreaks.DiseaseID = Diseases.idDisease WHERE \
        DiseaseID = " + diseaseID + " and \
        Cities.latitude >= " + minLat + " and \
        Cities.latitude <= " + maxLat + " and \
        Cities.longitude >= " + minLon + " and \
        Cities.longitude <= " + maxLon, res);
    }
});


// define another route
app.get('/test/:id?', (req, res) => {
    let filter = '';
    if(req.params.id) filter = ' WHERE id=' + parseInt(req.params.id);
    execSQLQuery('SELECT * FROM TestTable' + filter, res);
});


app.get('/query', (req, res) => {
    execSQLQuery("SELECT Diseases.Name as Disease, Outbreaks.NumberOfCases, Cities.name as City, States.name as State, Countries.name_pt as Country, Outbreaks.Date as Date FROM `Outbreaks` INNER JOIN `Cities` ON Outbreaks.CityID = Cities.cityID INNER JOIN `States` ON Cities.stateID = States.stateID INNER JOIN `Countries` ON States.CountryID = Countries.id INNER JOIN `Diseases` ON Outbreaks.DiseaseID = Diseases.idDisease WHERE DiseaseID = '5' and States.stateID = '35'", res);
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
          res.json("Consulta invalida");
          // res.json(error);
        else
          res.json(results);
        connection.end();
        console.log('Resultado exibido!');
    });
  }