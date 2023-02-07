const express = require('express');
const cors = require('cors');
const app = express();
const nconf = require('nconf');

nconf.argv();
nconf.env();

nconf.file({ file: (nconf.get('CONF_FILE') || './config.dev.json' ) });
console.log(`# Starting node service at host:\n\t port: ${nconf.get("PORT")} \n\t database: ${nconf.get("REDIS_DB_NUMBER")}`);

const serverPort = nconf.get('PORT') || 8086;

const EventService = require('./eventService');
const eventService = new EventService();


app.use(cors());
app.use(express.json())

app.post('/api/2.0/resources/wstudio', (req, res) => {
    eventService.addStudio(req.body, function (err, result) {
        res.json(result);
    });
});
app.get('/api/2.0/resources/wstudio/ts/:id', (req, res) => {
    const id = req.params.id;
    const idh = req.headers.id;
    console.log(`## GETALL body: ${id}`)
    console.log(`## GETALL head: ${idh}`)
    eventService.getStudioKeys(id, function (err, result) {
        res.json(result);
    });
});
app.get('/api/2.0/resources/wstudio/:id', (req, res) => {
    const id = req.params.id;
    const idh = req.headers.id;
    console.log(`## GETALL body: ${id}`)
    console.log(`## GETALL head: ${idh}`)
    eventService.getStudio(id, function (err, result) {
        res.json(result);
    });
});
app.get('/api/2.0/', (req, res) => {
    res.json({ status: 'started' });
});
app.get('/', (req, res) => {
    res.send('Event Service Node Node REST Server Started');
});
app.listen(serverPort, () => {
    console.log(`Event Node listening at http://localhost:${nconf.get('PORT')}`);
});

