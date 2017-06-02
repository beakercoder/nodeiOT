
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
            console.log('Begin BIGQUERY:');
            var messagedata = obj.data;
            //var content = {"nameid": "1658", "messagedata": " message.data "};
            insertRowsAsStream(datasetId,tableId,projectId,messagedata , message.attributes.published_at, message.attributes.device_id)
        }
    );



}

function insertRowsAsStream (datasetId, tableId, projectId, messagedata, publishedat, deviceid) {

    var firstfield = "deviceid";
var secondfield = "data";
var iotdata = String(messagedata);
//var content = JSON.stringify(firstfield) + ": " + JSON.stringify(deviceid) + "," + JSON.stringify(secondfield) + ": " + JSON.stringify(iotdata);
var content = {"deviceid": "67899" , "data": "67"}
//var c2=String(content);
console.log(content);
//var jsonrow={content};
//var jsonrow = JSON.parse(content);	
//console.log(jsonrow);     
//   builder = JSON.stringify(builder);
//    console.log(builder);
//    builder =  builder + ", messagedata:" + messagedata;
//    var content = "{" + builder + "}"; //deviceid": "1658", "messagedata": " + messagedata + "};
//	JSON.stringify(content);
   // console.log(content);
    // [START bigquery_insert_stream]
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
        .insert(content)
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
