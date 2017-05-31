
// [START app]
'use strict';

// [START setup]

var express = require('express');

var app = express();
var google = require('googleapis');
//var bigquery = google.bigquery('v2');

app.enable('trust proxy');

var Datastore = require('@google-cloud/datastore');
const BigQuery = require('@google-cloud/bigquery');
const projectId = "doolinhomeiot";
const datasetId = "OfficeData";
const tableId = "iotdata";

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
////google.auth.getApplicationDefault(function(err, authClient) {
////    if (err) {
////        console.log('Authentication failed because of ', err);
////        return;
////    }
////    if (authClient.createScopedRequired && authClient.createScopedRequired()) {
////        var scopes = ['https://www.googleapis.com/auth/cloud-platform'];
////        authClient = authClient.createScoped(scopes);
////    }});

//'insertId': 123456, 'json': '{"nameid": 123,"messagedata":"test1"

//var request = {projectId: projectId,datasetId: datasetId,tableID: tableId,resource: {"kind": "bigquery#tableDataInsertAllRequest","rows":
//    {}, auth: authClient]};

// [END setup]

function storeEvent(message) {
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
        }, function (err) {
            if (err) {
                console.log('There was an error storing the event', err);
            }
            console.log('stored in datastore', obj);
        }
    );
    //var response = "response";
    //var request = "{auth: authClient,'projectId':" + projectId + ", 'datasetId':" + datasetId + ", 'tableId':" + tableId + ", 'resource': {'kind': 'bigquery#tableDataInsertAllRequest','rows':[{'insertId': 123456, 'json': '{'nameid': '123','messagedata':'test1'}'}]}}";
    //save to BigQuery Tables
    //debug.log(request);
    ////bigquery.tabledata.insertAll(request, function (err, result) {
    ////   if (err) {
    ////       console.log(err);
    ////   } else {
    ////       console.log(result);
    ////   }
    ////});
    var content = "{'nameid': '789','messagedata': 'test1k'}";
    let rows = null;
    try {
        rows = JSON.parse(content)
    } catch (err) {
    }
    const bigquery = BigQuery({
        projectId: projectId
    });
    bigquery
        .dataset(datasetId);
        .table(tableId);
        .insertrow(rows);



}
    ////bigquery.tables.insert({
    ////    'projectId': projectId,
    ////    'datasetId': datasetId,
    ////    'tableId': tableId,
    ////    'resource': {'kind': 'bigquery#tableDataInsertAllRequest','rows':[{'insertId': 123456, 'json': '{"nameid": "123"","messagedata":"test1"}'}]}, function (err, result) {
    ////        if (err) {
     ////           console.log(err);
     ////       } else {
     ////           console.log(result);
      ////      }
      ////  }})};
//});

//err: result});


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
