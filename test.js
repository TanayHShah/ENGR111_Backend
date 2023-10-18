const { DynamoDBClient, PutItemCommand } = require("@aws-sdk/client-dynamodb");
const mqtt = require('mqtt');
const { uuid } = require('unique-string-generator');
const { ObjectId } = require('mongodb');

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
    console.log('Connected to MQTT broker');
    // Subscribe to all topics using the # wildcard
    client.subscribe('#');
});

client.on('error', (err) => {
    console.error('MQTT Error:', err);
});

client.on('message', (topic, message) => {
    // Process the received message and upload it to DynamoDB
    const newObjectId = new ObjectId();
    const params = {
        TableName: 'engr111_data_collection',
        Item: {
            id: newObjectId.toString(),
            topic: topic,
            payload: message.toString(),
            timestamp: new Date(),
        },
    };


    // dynamoDB.send(new PutItemCommand(params))
    //     .then(() => {
    //         console.log('Data uploaded to DynamoDB');
    //     })
    //     .catch((err) => {
    //         console.error('Error uploading to DynamoDB:', err);
    //     });
});
