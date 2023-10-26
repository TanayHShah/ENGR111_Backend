
const { DynamoDBClient, PutItemCommand } = require("@aws-sdk/client-dynamodb");
const mqtt = require('mqtt');
const { uuid } = require('unique-string-generator');
const { ObjectId, Int32 } = require('mongodb');


// AWS DynamoDB configuration
const dynamoDBConfig = {
    region: 'us-east-1',
    credentials: {
        accessKeyId: 'AKIARNO54IOA37WAHO6A',
        secretAccessKey: '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',
    },
};

const dynamoDB = new DynamoDBClient(dynamoDBConfig);

const tlsOptions = {
    rejectUnauthorized: false,
};


// MQTT configuration
const client = mqtt.connect('mqtt://98db5050a791439c98eac188febfecbe.s2.eu.hivemq.cloud', {
    username: 'stevens',
    password: 'Stevens@1870',
    protocol: 'mqtts', // Specify the MQTT protocol as 'mqtts' for secure communication
    ...tlsOptions,
});

// Connect to MQTT broker
client.on('connect', () => {
    // Subscribe to all topics using the # wildcard
    client.subscribe('#');
});

client.on('error', (err) => {
    console.error('MQTT Error:', err);
});

client.on('message', (topic, message) => {
    // Process the received message and upload it to DynamoDB
    const newObjectId = new ObjectId();
    timestamp = Math.floor((new Date()).getTime() / 1000)

    const params = {
        TableName: 'engr111_data_collection',
        Item: {
            id: { S: newObjectId.toString() },
            topic: { S: topic },
            payload: { S: message.toString() },
            timestamp: {'N': String(timestamp)},
        },
    };


    dynamoDB.send(new PutItemCommand(params))
        .then(() => {
        })
        .catch((err) => {
            console.error('Error uploading to DynamoDB:', err);
        });
});
