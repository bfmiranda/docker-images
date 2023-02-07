
const redis = require('redis');
const redisearch = require('redis-redisearch');
const nconf = require('nconf');


const indexName = process.env.REDIS_INDEX || 'ibm:search:index:ibm:wstudio';
const providerKeyPrefix = process.env.REDIS_EVENTS_PREFIX || 'ibm';
const resourceKeyPrefix = process.env.RESOURCE_PREFIX || 'watson-studio';


nconf.argv();
nconf.env();

nconf.file({ file: (nconf.get('CONF_FILE') || './config.dev.json' ) });

const redisHost = process.env.REDIS_HOST || nconf.get("REDIS_HOST");
const redisPort = process.env.REDIS_PORT || nconf.get("REDIS_PORT");
const redisDbNumber = process.env.REDIS_DB_NUMBER || nconf.get("REDIS_DB_NUMBER");
const redisPassword = process.env.REDIS_PASSWORD || nconf.get("REDIS_PASSWORD");

let redisUrl = `redis://${redisHost}:${redisPort}/${redisDbNumber}`
console.log(`\t Redis : ${redisUrl}`);

if (redisPassword) {
    redisUrl = `redis://default:${redisPassword}@${redisHost}:${redisPort}/${redisDbNumber}`
}


redisearch(redis);
const client = redis.createClient(redisUrl);

const EventService = function () {

    const _getStudio = function(id, callback) {
        // using hgetall, since the hash size is limited
        
        client.hgetall(id, function(err, res) {
            console.log(`######################## Service: \n => ${JSON.stringify(res)}`)
            callback( err, res );
        });
    }

    const _getStudioKeys = function(id, callback) {
        // using hgetall, since the hash size is limited
        
        client.key(id, function(err, res) {
            console.log(`######################## Service: \n => ${JSON.stringify(res)}`)
            callback( err, res );
        });
    }

    const _deleteComment = function(id, callback) {
        // using hgetall, since the hash size is limited
        client.del(id, function(err, res) {
            callback( err, res );
        });
    }


    const _addStudio = function(event, callback) {
        const ts = Date.now();
        const key = `${providerKeyPrefix}:${resourceKeyPrefix}:${event.studio_id}:${ts}`

        event.timestamp = ts;

        console.log(`######################## Service: \n => ${JSON.stringify(event)}`)

        const values = [
            "studio_id" , event.studio_id,
            "user_id" , event.user_id,
            "event" , event.event,
            "timestamp" , event.timestamp,
        ];
        client.hmset(key, values, function(err, res) {
            callback( err, { "id" : key, "event" : event  } );
        });
    }

    /**
     * Retrieve the list of comments for a movie
     * @param {*} id 
     * @param {*} options 
     * @param {*} callback 
     */
    // const _getEventComments = function(id, options, callback) {

    //     let offset = 0; // default values
    //     let limit = 10; // default value

    //     const queryString = `@movie_id:[${id} ${id}]`

    //     // prepare the "native" FT.SEARCH call
    //     // FT.SEARCH IDX_NAME queryString  [options]
    //     const searchParams = [
    //         indexName,    // name of the index
    //         queryString,  // query string
    //         'WITHSCORES'  // return the score
    //     ];

    //     // if limit add the parameters
    //     if (options.offset || options.limit) {
    //         offset = options.offset || 0;
    //         limit = options.limit || 10
    //         searchParams.push('LIMIT');
    //         searchParams.push(offset);
    //         searchParams.push(limit);
    //     }
    //     // if sortby add the parameters  
    //     if (options.sortBy) {
    //         searchParams.push('SORTBY');
    //         searchParams.push(options.sortBy);
    //         searchParams.push((options.ascending) ? 'ASC' : 'DESC');
    //     }

    //     client.ft_search(
    //         searchParams,
    //         function (err, searchResult) {

    //             const totalNumberOfDocs = searchResult[0];
    //             const result = {
    //                 meta: {
    //                     totalResults: totalNumberOfDocs,
    //                     offset,
    //                     limit,
    //                     queryString,
    //                 },
    //                 docs: [],
    //             }

    //             // create JSON document from n/v pairs
    //             for (let i = 1; i <= searchResult.length - 1; i++) {
    //                 const doc = {
    //                     meta: {
    //                         score: Number(searchResult[i + 1]),
    //                         id: searchResult[i]
    //                     }
    //                 };
    //                 i = i + 2;
    //                 doc.fields = {};
    //                 const fields = searchResult[i]
    //                 if (fields) {
    //                     for (let j = 0, len = fields.length; j < len; j++) {
    //                         const idxKey = j;
    //                         const idxValue = idxKey + 1;
    //                         j++;
    //                         doc.fields[fields[idxKey]] = fields[idxValue];

    //                         // To make it easier let's format the timestamp
    //                         if (fields[idxKey] == "timestamp") {
    //                             const date = new Date(parseInt(fields[idxValue]));
    //                             doc.fields["dateAsString"] =  date.toDateString()+" - "+date.toLocaleTimeString() ;
    //                         }
    //                     }
    //                 }
    //                 result.docs.push(doc);
    //             }

    //             callback(err, result);
    //         }
    //     );
    // }

    return {
        getStudio: _getStudio,
        //getMovieComments: _getMovieComments,
        addStudio: _addStudio,
        deleteComment: _deleteComment,
    };
}

module.exports = EventService;

