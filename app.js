// Requiring module
const express = require('express');

// Creating express object
const app = express();
app.use(express.json());


// Port Number
const PORT = process.env.PORT ||5000;

// Server Setup
app.listen(PORT,console.log(
`Server started on port ${PORT}`));
