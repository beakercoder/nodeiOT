
// [START app]
'use strict';

// [START setup]

var express = require('express');

var app = express();
var google = require('googleapis');


app.enable('trust proxy');

var Datastore = require('@google-cloud/datastore');

const projectId = "doolinhomeiot";
const datasetId = "OfficeData";
const tableId = "iotdata";

// Instantiate a datastore client
var datastore = Datastore();

var PubSub = require('@google-cloud/pubsub');

//Instantiate a pubsub client
var pubsub = PubSub();


var PUBSUB_VERIFICATION_TOKEN = process.env.PUBSUB_VERIFICATION_TOKEN;

var topic = pubsub.topic(process.env.PUBSUB_TOPIC);

var subscription = pubsub.subscription(process.env.PUBSUB_SUBSCRIPTION_NAME);

// [END setup]

function storeEvent(message) {
    var key = datastore.key('officedata');
    key.identifier = "id"


    var obj = {
        messageid: message.id,
        deviceid: message.attributes.device_id,
        ////event: message.attributes.event,
        published: message.attributes.published_at,
        data: message.data
    }
    datastore.save(
        {
            key: key,
            data: obj
        }, function (err) {
            if (err) {
                console.log('There was an error storing the event', err);
            }
            console.log('stored in datastore', obj);
            var messagedata = obj.data;
            insertRowsAsStream(datasetId,tableId,projectId,messagedata , message.attributes.published_at, message.attributes.device_id)
        }
    );



}

function insertRowsAsStream (datasetId, tableId, projectId, messagedata, publishedat, deviceid) {

    var firstfield = "deviceid";
var secondfield = "data";
var thirdfield = "timestamp";
var fourthfield = "temperature";
var fifthfield = "humidity";
var temp = messagedata.split(".")
var tempstring = temp[0].toString();
var humstring = temp[1].toString();
var hum = messagedata.split(",");
var humstring = hum[1];
humstring = humstring.substring(0,2);
console.log("TEMP=", tempstring);
console.log("HUM=", humstring);
var iotdata = String(messagedata);
var content = "{" + JSON.stringify(firstfield) + ": " + JSON.stringify(deviceid) + "," + JSON.stringify(secondfield) + ": " + JSON.stringify(iotdata) + "," + JSON.stringify(thirdfield) + ": " + JSON.stringify(publishedat) + ", " ;
content = content + JSON.stringify(fourthfield) + ": " + JSON.stringify(tempstring) + ",";
content = content + JSON.stringify(fifthfield) + ": " + JSON.stringify(humstring) + "}";
//var content = {"deviceid": "67899" , "data": "67"}
var jsonrow={content};
var jsonrow = JSON.parse(content);	
console.log("JSON=", {jsonrow});     
    // Imports the Google Cloud client library
    const BigQuery = require('@google-cloud/bigquery');

    // Instantiates a client
    const bigquery = BigQuery({
        projectId: projectId
    });
    // Inserts data into a table
    bigquery
        .dataset(datasetId)
        .table(tableId)
        .insert(jsonrow)
        .then((insertErrors) => {
        console.log('Inserted:');

})
.catch((err) => {
        console.error('ERROR:', err);
});
    // [END bigquery_insert_stream]
}

subscription.on('message', function (message) {
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
