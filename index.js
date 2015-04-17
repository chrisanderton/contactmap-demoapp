var Sequelize = require('sequelize');
var async = require('async');
var HashMap = require('hashmap');
var geocode = require('./geocode');
var rgeocode = require('./rgeocode');
var _ = require('lodash');

// ************** DATABASE MODELS ***********************

var dburl = process.env.DATABASE_URL || 'postgres://localhost/herokuconnect';
var db = new Sequelize(dburl, {
				dialectOptions: {
					ssl: dburl.indexOf('localhost') == -1
				},
				logging: false
			 });


var Contact = db.define('Contact', {
	id: Sequelize.INTEGER,
	sfid: Sequelize.STRING,
	email: Sequelize.STRING,
	name: Sequelize.STRING,
	mailingcity: Sequelize.STRING,
	mailingstate: Sequelize.STRING,
	mailingstreet: Sequelize.STRING
}, {
	timestamps: false,
	freezeTableName: true,
	schema: 'salesforce',
	tableName: 'contact'
	}
);

Account = db.define('Account', {
	id: Sequelize.INTEGER,
	sfid: Sequelize.STRING,
	billingcity: Sequelize.STRING,
	billingcountry: Sequelize.STRING
}, {
	timestamps: false,
	freezeTableName: true,
	schema: 'salesforce',
	tableName: 'account'
	}
);


Geocode = db.define('geocode', {
    id: Sequelize.INTEGER,
    address: Sequelize.STRING,
    lat: Sequelize.FLOAT,
    lon: Sequelize.FLOAT
});

// Create geocode cache table if not exists
db.sync();

// ************** GEOCODING LOGIC ***********************

var contact_locations = [];

function geocode_contact(contact, callback) {
	if (contact.values.mailingstreet ||
		contact.values.mailingcity ||
		contact.values.mailingstate) {

		var addr = contact.values.mailingstreet + "," +
					contact.values.mailingcity + "," +
					contact.values.mailingstate;

		geocode(addr, function(geocode) {
			if (geocode) {
				callback(null, {name: escape(contact.values.name), lat: geocode.lat, lon:geocode.lon});
			} else {
				callback();
			}
		});
	} else {
		callback();
	}
}


function geocode_country(country, callback) {
	if (country) {
		geocode(country, function(geocode) {
			if (geocode) {
				callback(null, {country: escape(country), lat: geocode.lat, lon: geocode.lon, count: 0});
			} else {
				callback();
			}
		});
	} else {
		callback();
	}
}



var account_locations = [];
function geocode_account(account, callback) {
	if (account.values.billingcountry ) {

		var addr = account.values.billingcountry;

		geocode(addr, function(geocode) {
			if (geocode) {
				callback(null, {name: escape(account.billingcountry), lat: geocode.lat, lon: geocode.lon});
			} else {
				callback();
			}
		});
	} else {
		callback();
	}
}

var account_counts = [];
function count_account(count, country, callback) {
	console.log("INSIDE ACCOUNT COUNTS");

	if (country ) {

		var addr = account.values.billingcountry;
		callback(null, {country: escape(country), count:count});

	} else {
		callback();
	}
}


function load_accounts(callback) {
	Account.findAll({where: { billingcountry: ['DE','NL','GB']} , limit: 100}).then(function(rows) {
		async.map(rows, geocode_account, callback);
	});
}



function load_contacts(callback) {
	Contact.findAll({limit:200}).then(function(rows) {
		async.map(rows, geocode_contact, callback);
	});
}

function load_countries(callback) {
	async.map(["DE","NL","GB", "PL", "FR"], geocode_country, callback);
}


// EXPRESS

var express = require('express');
var app = express();

app.set('port', (process.env.PORT || 5000));
app.use(express.static(__dirname + '/public'));
app.set('views', './views');
app.set('view engine', 'ejs');

app.get('/', function(req, res) {
	load_contacts(function(error, contact_locations) {
		console.log("Locations: ", contact_locations);
		res.render('index', {contact_locations: contact_locations.filter(function(val) { return val })});
	});
});


app.get('/updatecountry', function(req, res) {
	console.log("/updatecountry ");
	console.log("REQUEST QUERY: ************ " + JSON.stringify(req.query));
	//{"current":"NL","lat":"52.024601490848866","lng":"19.52954724999995"}

	var geo = req.query.lat+','+req.query.lng;

	console.log("***************** geo: " + geo);

	rgeocode(geo, function(rgeocode) {
		if (rgeocode) {
			console.log("RGEOCODE ************ : " + JSON.stringify(rgeocode));
			console.log("RGEOCODE short************ : " + JSON.stringify(rgeocode.short));
			console.log("RGEOCODE long************ : " + JSON.stringify(rgeocode.long));


			Account.update(
				 {
				    billingcountry: rgeocode.short
				 },
				 {
				    where: { billingcountry : req.query.currentCountry }
				 })
				 .success(function (result) {
					console.log("UPDATE RESULTS ********************** " + result);

					res.send(result);
				 })
				 .error(function (error) {
					console.log("UPDATE error ********************** " + error);

				 });

			};


	});



});


app.get('/accounts', function(req, res) {
	load_countries(function(error, countryGeocodes) {
        
        var countryData = _.indexBy(countryGeocodes, 'country');

        // this is used for interpolation as i couldn't see how to get sequelize to inject an array nicely
        var countries = _.keys(countryData).map(function(country) { return "'" + country + "'"}).join(",");
        
        var query = 'SELECT UPPER(billingcountry) as country, count(*) as count FROM salesforce.account WHERE billingcountry IN (' + countries + ') GROUP BY upper(billingcountry)';
        
        db.query(
            query,
            { 
                type: db.QueryTypes.SELECT, 
                raw: true, 
                logging: console.log
            }
        ).then(function(countryCounts) {
            _.each(countryCounts, function(countryCount) { countryData[countryCount["country"]]["count"] = parseInt(countryCount["count"], 10) });
        }).then(function() {
           res.render('accounts', {accountloc_geomap: countryData}); 
        });
	});
});


app.get('/create', function(req, res){
  var create_url = 'https://connect.heroku.com/dashboard-next/create-connection';
  // Redirect to Heroku Connect dashboard to finish setup
  var hostRe = new RegExp(/^([^.]+)\.herokuapp\.com$/);

  var match = req.headers.host.match(hostRe);

  if (match) {
    res.redirect(create_url+'?create='+match[1]);
  } else {
    res.status(400).send("You need to be running on Heroku!");
  }
});

app.listen(app.get('port'), function() {
  console.log("Node app is running at localhost:" + app.get('port'));
});
