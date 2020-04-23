const express = require('express');
const bodyParser = require('body-parser');

// create express app
const app = express();

// parse requests of content-type - application/x-www-form-urlencoded
app.use(bodyParser.urlencoded({ extended: true }))

// parse requests of content-type - application/json
app.use(bodyParser.json())

// define a simple route
app.get('/', (req, res) => {
    res.json({"message": "Welcome to Epidemics API. For information on how to use, visit: https://github.com/gsalibi/epidemics-api"});
});

// listen for requests
app.listen(3000, () => {
    console.log("Server is listening on port 3000");
});