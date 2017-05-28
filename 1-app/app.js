
// [START app]
'use strict';

// [START setup]

var express = require('express');

var app = express();


app.enable('trust proxy');

var Datastore = require('@google-cloud/datastore');
const BigQuery = require('@google-cloud/bigquery');
const projectId = "doolinhomeiot";
const datasetId = "OfficeData";
const tableId = "iotdata";
const bigquery = BigQuery({
  projectId: projectId
});

// Instantiate a datastore client
var datastore = Datastore();

var PubSub = require('@google-cloud/pubsub');

//Instantiate a pubsub client
var pubsub = PubSub();

//The following environment variables are set by app.yaml when running on GAE,
//but will need to be manually set when running locally.
var PUBSUB_VERIFICATION_TOKEN = process.env.PUBSUB_VERIFICATION_TOKEN;

var topic = pubsub.topic(process.env.PUBSUB_TOPIC);

var subscription = pubsub.subscription(process.env.PUBSUB_SUBSCRIPTION_NAME);
// Instantiates a client

// [END setup]

function storeEvent(message) 
{
    var key = datastore.key('officedata');
	key.identifier = "id"
    // You can uncomment some of the other things if you want to store them in the database

    var obj = {
		messageid: message.id
		//deviceid: message.attributes.device_id,
		////event: message.attributes.event,
		//published: message.attributes.published_at,
	    	//data: message.data
	}
    datastore.save(
    {
        key: key,
        data: obj
    }, function(err) 
    {
		if(err) 
		{
			console.log('There was an error storing the event', err);
		}
		console.log('stored in datastore', obj);
    }
    );
bigquery.tabledata.insertAll({
  auth: oauth2Client,
  'projectId': projectId,
  'datasetId': datasetId,
  'tableId': tableId,
  'resource ': {"kind": "bigquery#tableDataInsertAllRequest","rows": 
    [
      {
        "insertId": 123456,
        "json": '{"id": 123,"name":"test1"}'
      }
    ]
  }
}, function(err, result) 
    {
    if (err) 
    {
        return console.error(err);
    }
        console.log(result);
    }
);

}

subscription.on('message', function(message) {
	console.log('event received', message);
	// Called every time a message is received.
	// message.id = ID used to acknowledge its receival.
	// message.data = Contents of the message.
	// message.attributes = Attributes of the message.
	storeEvent(message);
	message.ack();
});

// [START listen]
var server = app.listen(process.env.PORT || 8080, function () {
  console.log('App listening on port %s', server.address().port);
  console.log('Press Ctrl+C to quit.');
});
// [END listen]
// [END app]

module.exports = app;
