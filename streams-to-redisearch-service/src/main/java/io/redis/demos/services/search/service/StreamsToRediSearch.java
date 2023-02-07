package io.redis.demos.services.search.service;

import io.redis.demos.services.search.service.schemas.ActorsSchema;
import io.redis.demos.services.search.service.schemas.CommentsSchema;
import io.redis.demos.services.search.service.schemas.KeysPrefix;
import io.redis.demos.services.search.service.schemas.MoviesSchema;
import io.redisearch.*;
import io.redisearch.aggregation.AggregationBuilder;
import io.redisearch.aggregation.SortedField;
import io.redisearch.aggregation.reducers.Reducers;
import io.redisearch.client.AutoCompleter;
import io.redisearch.client.Client;
import io.redisearch.client.IndexDefinition;
import io.redisearch.client.SuggestionOptions;
import lombok.extern.slf4j.Slf4j;
import org.springframework.beans.factory.annotation.Value;
import org.springframework.stereotype.Component;
import redis.clients.jedis.*;
import redis.clients.jedis.exceptions.JedisDataException;

import javax.annotation.PostConstruct;
import java.net.URI;
import java.net.URISyntaxException;
import java.util.*;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.Future;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;

import static java.util.stream.Collectors.toMap;

@Slf4j
@Component
public class StreamsToRediSearch extends KeysPrefix {

    private final static String SUGGEST_OP = "SUGG";
    private final static String INDEX_OP = "IDX";

    private final static String DOC_PREFIX_MOVIE = "ms:docs:movies:";
    private final static String DOC_PREFIX_ACTOR = "ms:docs:actors:";
    private final static String DOC_PREFIX_MOVIE_COMMENTS = "ms:comments:movie:";

    @Value("${redis.host}")
    private String redisHost;

    @Value("${redis.port}")
    private int redisPort;

    @Value("${redis.password}")
    private String redisPassword;

    // Stream lists
    @Value("${redis.streams}")
    private List<String> streamList;

    // Stream lists
    @Value("${redis.groupnameprefix}")
    private String groupNamePrefix;

    // Consumer name of this instance
    @Value("${redis.consumer}")
    private String consumer;


    Future<?> streamProcessingTask;
    private String status = "STOPPED";
    private boolean suggest = true;
    private boolean fulltext = true;
    String groupNameSuggest = null;
    String groupNameSearch = null;


    private JedisPool jedisPool;
    private Map<String, Client> suggestClients = new HashMap<>();
    private Map<String, Client> searchClients = new HashMap<>();


    public StreamsToRediSearch() {
        log.info(" === StreamsToRediSearch Started : Waiting for user action  ===");
    }

    /**
     * This method check of the index exist, if not it is created
     * @param itemName
     * @param autocomplete
     */
    private void checkRediSearchSchema(String itemName, boolean autocomplete){

        String searchKey = SEARCH_INDEX_PREFIX + itemName;
        log.info("Prepare index {} ", searchKey);
        Client c = new Client( searchKey, jedisPool );
        searchClients.put(searchKey, c);


        if (autocomplete) {
            // suggestion
            String suggKey = SUGGEST_PREFIX + itemName;
            Client cSuggest = new Client( suggKey , jedisPool ); // TODO: check if still needed
            suggestClients.put(suggKey, cSuggest);
        }

        // check if search index exists if not create it
        try {
            c.getInfo();
        } catch(JedisDataException jde) {
            if (jde.getMessage().equalsIgnoreCase("Unknown Index name")) {
                log.info("Create Index : {}", itemName);
                // TODO : Make this dynamic, from schema definition
                if ( itemName.equalsIgnoreCase("movies")) {
                    IndexDefinition indexDefinition = new IndexDefinition()
                            .setPrefixes(new String[] {DOC_PREFIX_MOVIE});
                    c.createIndex(MoviesSchema.getSchema(), Client.IndexOptions.defaultOptions().setDefinition(indexDefinition));
                }
                else if ( itemName.equalsIgnoreCase("actors")) {
                    IndexDefinition indexDefinition = new IndexDefinition()
                            .setPrefixes(new String[] {DOC_PREFIX_ACTOR});
                    c.createIndex(ActorsSchema.getSchema(), Client.IndexOptions.defaultOptions().setDefinition(indexDefinition));
                }
                else if ( itemName.equalsIgnoreCase("comments:movies")) {
                    IndexDefinition indexDefinition = new IndexDefinition()
                            .setPrefixes(new String[] {DOC_PREFIX_MOVIE_COMMENTS});
                    c.createIndex(CommentsSchema.getSchema(), Client.IndexOptions.defaultOptions().setDefinition(indexDefinition));
                }

            } else {
                log.warn(jde.getMessage());
                throw  jde;
            }
        }

    }

    @PostConstruct
    private void init(){

        groupNameSuggest = groupNamePrefix.concat(".suggest");
        groupNameSearch = groupNamePrefix.concat(".index");

        log.info("Create Jedis Pool with {}:{} ", redisHost, redisPort);
        if (redisPassword != null && redisPassword.trim().isEmpty()) {
            redisPassword = null;
        }
        jedisPool = new JedisPool(new JedisPoolConfig(), redisHost, redisPort, 5000, redisPassword );


        // Using stream to create the index (for CDC based data)
        streamList.forEach(streamKey -> {
            String[] s = streamKey.split(":");
            String itemName = s[s.length-1];
            checkRediSearchSchema(itemName, true);
        });

        // add an index for Comments, that is not related to any Stream
        checkRediSearchSchema("comments:movies", false);

        log.info("Will look at {} streams", streamList);
    }

    /**
     *
     * @return
     */
    private void createConsumerGroups(Jedis jedis) {
        log.info("create groups for search & suggest for each streams");

        streamList.forEach( streamKey -> {
            String[] s = streamKey.split(":");
            String itemName = s[s.length-1];
            StreamEntryID streamEtnryId = new StreamEntryID("0-0");

            // Suggest
            try {
                // Create consumer if does not exist already
                jedis.xgroupCreate(
                        streamKey,
                        groupNameSuggest,
                        streamEtnryId,
                        true
                );
                log.info(" Consumer Group {} / {} created", streamKey, groupNameSuggest);

            } catch (JedisDataException jde) {
                if (jde.getMessage().startsWith("BUSYGROUP")) {
                    log.info(" Consumer Group {} / {} already exists", streamKey, groupNameSuggest);
                }
            }

            // Search Fulltext Index
            try {
                // Create consumer if does not exist already
                jedis.xgroupCreate(
                        streamKey,
                        groupNameSearch,
                        streamEtnryId,
                        true
                );
                log.info(" Consumer Group {} / {} created", streamKey, groupNameSearch);

            } catch (JedisDataException jde) {
                if (jde.getMessage().startsWith("BUSYGROUP")) {
                    log.info(" Consumer Group {} / {} already exists", streamKey, groupNameSearch);
                }
            }

        });



    }


    public Map<String,String> processStream() {
        init(); // TODO : not the best but simple for now to be sure that all resources are newly created
        Map<String, String> result =  new HashMap<>();
        ExecutorService executor = Executors.newSingleThreadExecutor();
        streamProcessingTask = executor.submit(this::processStreamInThread);
        result.put("msg", "Stream Reading started");
        status = "RUNNING";
        return result;
    }

    /**
     * Stop the stream reader
     * @return
     */
    public Map<String,String> stopProcessStream(){
        log.info("Stopping Stream Processing");
        Map<String, String> result =  new HashMap<>();
        if (streamProcessingTask != null) {
            streamProcessingTask.cancel(true);
            result.put("msg", "Stream Reading Stopped");
        }
        return result;
    }

    public String getState() {
        return this.status;
    }

    public String getStatus() {
        return status;
    }

    public boolean isSuggest() {
        return suggest;
    }

    public void setSuggest(boolean suggest) {
        this.suggest = suggest;
    }

    public boolean isFulltext() {
        return fulltext;
    }

    public void setFulltext(boolean fulltext) {
        this.fulltext = fulltext;
    }

    /**
     * This method:
     *    - stop the process
     *    - changes the status of the full text service
     **
     * @return map with the new configuration
     */
    public Map<String,String> changeFullText() {
        Map<String,String> result =  new HashMap<>();
        this.stopProcessStream();
        this.setFulltext( ! this.isFulltext() );
        result.put("fulltext", String.valueOf(this.isFulltext()));
        result.put("status", String.valueOf(this.getState()));
        return result;
    }

    /**
     * This method:
     *    - stop the process
     *    - changes the status of the full text service
     **
     * @return map with the new configuration
     */
    public Map<String,String> changeSuggest() {
        log.info("Suggest status before change {} ", this.isSuggest());
        Map<String,String> result =  new HashMap<>();
        this.stopProcessStream();
        this.setSuggest( ! this.isSuggest() );
        result.put("suggest", String.valueOf(this.isSuggest()));
        result.put("status", String.valueOf(this.getState()));
        log.info("Suggest status after change {} ", this.isSuggest());
        return result;
    }

    /**
     * TODO: Make this generic to support dynamic service binding
     *       for new datatype based on stream name
     */
    public void processStreamInThread() {
        log.info(" start stream processing");
        if (streamList.isEmpty()) {
            final String msg = "No stream to process";
            log.warn(msg);
        } else {
            final String msg = String.format("Will process %s ", streamList);
            log.warn(msg);


            Jedis jedis = null;
            try {
                jedis = jedisPool.getResource();

                createConsumerGroups(jedis);

                Map.Entry<String, StreamEntryID>[] xReadQueries = (Map.Entry<String, StreamEntryID>[]) new Map.Entry[streamList.size()];
                Jedis finalJedis = jedis;
                final int[] streamCtr = { 0 };

                // Prepare read queries for each group
                streamList.forEach(stream -> {
                    StreamEntryID streamEtnryId = new StreamEntryID("0-0");
                    Map.Entry<String, StreamEntryID> queryStream = new AbstractMap.SimpleImmutableEntry<>(stream, StreamEntryID.UNRECEIVED_ENTRY);
                    xReadQueries[streamCtr[0]] = queryStream;
                    streamCtr[0]++;
                });

                boolean loop = true;
                try {
                    while (loop) {
                        loop = !streamProcessingTask.isCancelled();

                        // consume messages for full text
                        if (isFulltext()) {
                            List<Map.Entry<String, List<StreamEntry>>> eventsFullText = jedis.xreadGroup(
                                    groupNameSearch,
                                    consumer,
                                    1,
                                    0,
                                    false,
                                    xReadQueries
                            );

                            if (eventsFullText != null) {
                                for (Map.Entry m : eventsFullText) {
                                    if (m.getValue() instanceof ArrayList) {
                                        List<StreamEntry> l = (List) m.getValue();
                                        if (l.size() != 0) {
                                            Map<String, String> data = l.get(0).getFields();
                                            log.info(">>> {}", data.get("source.table"));
                                            String operation = data.get("source.operation").toUpperCase();
                                            if (data.get("source.table").equalsIgnoreCase("movies")) {
                                                updateSearchForMovie(data, operation);
                                                jedis.xack(m.getKey().toString(), groupNameSearch, l.get(0).getID());
                                            }
                                            if (data.get("source.table").equalsIgnoreCase("actors")) { // create/update actor
                                                updateSearchForActor(data, operation);
                                                jedis.xack(m.getKey().toString(), groupNameSearch, l.get(0).getID());
                                            }
                                        }
                                    }
                                }
                            } else {
                                log.debug("no event in stream - ");
                                try {
                                    // if we have to process 2 types of event make it shorter
                                    if (isSuggest()) {
                                        TimeUnit.MILLISECONDS.sleep(100);
                                    } else {
                                        TimeUnit.MILLISECONDS.sleep(500);
                                    }
                                } catch (InterruptedException e1) {
                                }
                            }
                        }

                        // consume messages for suggest
                        if (isSuggest()) {
                            List<Map.Entry<String, List<StreamEntry>>> eventsSuggest = jedis.xreadGroup(
                                    groupNameSuggest,
                                    consumer,
                                    1,
                                    0,
                                    false,
                                    xReadQueries
                            );

                            if (eventsSuggest != null) {
                                for (Map.Entry m : eventsSuggest) {
                                    if (m.getValue() instanceof ArrayList) {
                                        List<StreamEntry> l = (List) m.getValue();
                                        if (l.size() != 0) {
                                            Map<String, String> data = l.get(0).getFields();
                                            String operation = data.get("source.operation").toUpperCase();
                                            if (data.get("source.table").equalsIgnoreCase("movies")) {
                                                updateSuggestForMovie(data, operation);
                                                jedis.xack(m.getKey().toString(), groupNameSuggest, l.get(0).getID());
                                            }
                                            if (data.get("source.table").equalsIgnoreCase("actors")) { // create/update actor
                                                updateSuggestForActor(data, operation);
                                                jedis.xack(m.getKey().toString(), groupNameSuggest, l.get(0).getID());
                                            }
                                        }
                                    }
                                }
                            } else {
                                log.debug("no event in stream - ");
                                try {
                                    // if we have to process 2 types of event make it shorter
                                    if (isFulltext()) {
                                        TimeUnit.MILLISECONDS.sleep(100);
                                    } else {
                                        TimeUnit.MILLISECONDS.sleep(500);
                                    }
                                } catch (InterruptedException e1) {
                                }
                            }
                        }

                    }
                } catch (JedisDataException jde) {
                    log.warn( jde.getMessage() );
                }
            } catch (Exception e) {
                log.warn( e.getMessage() );
            } finally {
                if (jedis != null) {
                    jedis.close();
                }
                log.info("Leaving the stream processing method");
                status = "STOPPED";
            }
        }
    }

    /**
     * Update Redis suggestion
     * @param newValue the string to index, null when delete
     * @param oldValue the string to remove or remove
     */
    private void updateAutocomplete(String index, String newValue, String oldValue, String id) {
        if (suggest) {
            String complexIndexName = SUGGEST_PREFIX + index; // TODO: hard coded values....
            Client search = suggestClients.get(complexIndexName);

            if ( newValue != null && oldValue == null  ) { // creation
                log.info("CREATE FTS entry for {} in {}", newValue, index);
                Suggestion suggestion = Suggestion.builder().str(newValue)
                        .payload(id)
                        .build();
                search.addSuggestion(suggestion, true);
            } else if ( newValue != null && oldValue != null ) { // update
                if (oldValue.equals(newValue) ) {
                    log.info("UPDATE FTS Old and New values ({}) are identical, no update of the index {}", newValue, index);
                } else{
                    log.info("UPDATE FTS entry for {} in {}", newValue, index);
                    // TODO : wait for https://github.com/RediSearch/JRediSearch/issues/89
                    Jedis jedis = null;
                    try {
                        jedis = jedisPool.getResource();
                        Suggestion suggestion = Suggestion.builder().str(newValue).payload(id).build();
                        search.addSuggestion(suggestion, true);
                        Object result = jedis.sendCommand(AutoCompleter.Command.SUGDEL, index, oldValue);
                    } catch (Exception e) {
                        e.printStackTrace();
                    } finally {
                        if (jedis != null) {
                            jedis.close();
                        }
                    }
                }
            } else if ( newValue == null && oldValue != null ) { // delete
                log.info("DELETE FTS entry for {} in {}", oldValue, index);
                // TODO : wait for https://github.com/RediSearch/JRediSearch/issues/89
                Jedis jedis = null;
                try {
                    jedis = jedisPool.getResource();
                    Object result = jedis.sendCommand(AutoCompleter.Command.SUGDEL, index, oldValue  );
                } catch (Exception e) {
                    e.printStackTrace();
                }
                finally {
                    if (jedis != null) {
                        jedis.close();
                    }
                }
            }
        }
    }

    /**
     * Return the suggestion based on terms
     * @param term Search index
     * @return
     */
    public List<Map<String,Object>> suggest(String indexName, String term){
        List<Map<String,Object>> result = new ArrayList<>();
        String complexIndexName = SUGGEST_PREFIX + indexName;
        log.info("Suggestion query on {} with {}", complexIndexName, term);
        Client search = suggestClients.get(complexIndexName);

        if (search != null) {
            List<Suggestion> suggestions = search.getSuggestion(term, SuggestionOptions.builder().max(100).with(SuggestionOptions.With.PAYLOAD).build() );

            result = suggestions
                    .stream()
                    .map( s -> {
                        Map<String,Object> m = new HashMap<>();
                        m.put("string", s.getString());
                        m.put("id", s.getPayload());
                        m.put("score", s.getScore());
                        return m;
                    } )
                    .collect(Collectors.toList());
        }
        return result;
    }

    /**
     *
     * @param indexName
     * @param q
     * @param offset
     * @param limit
     * @return
     */
    public Map<String,Object> search(String indexName, String q, int offset, int limit) {
        Map<String,Object> result = new HashMap<>();
        Map<String,Object> resultMeta = new HashMap<>();

        String complexIndexName = "ms:search:index:" + indexName; // TODO: hard coded values....
        Client client = searchClients.get(complexIndexName);

        log.info("Search `{}` with `{}` ", complexIndexName, q);

        Query query = new Query(q)
                        .limit(offset, limit);

        long start = System.currentTimeMillis();
        // Execute the query
        SearchResult queryResult = client.search(query);
        long end = System.currentTimeMillis();

        // Adding the query string for information purpose
        resultMeta.put("queryString",query);

        // Get the total number of documents and other information for this query:
        resultMeta.put("totalResults", queryResult.totalResults);
        resultMeta.put("elaspedTimeMs",  end - start );
        resultMeta.put("offset", offset);
        resultMeta.put("limit", limit);

        result.put("meta", resultMeta);

        //result.put("totalResults", queryResult.totalResults);
        //result.put("elaspedTimeMs", end - start);
        List<Map<String, Object>> docsToReturn = new ArrayList<>();


        // remove the properties array and create attributes
        List<Document> docs =  queryResult.docs;

        for (Document doc :docs) {

            Map<String,Object> props = new HashMap<>();
            Map<String,Object> meta = new HashMap<>();
            meta.put("id", doc.getId());
            meta.put("score", doc.getScore());
            doc.getProperties().forEach( e -> {
                props.put( e.getKey(), e.getValue() );
            });

            Map<String,Object> docMeta = new HashMap<>();
            docMeta.put("meta",meta);
            docMeta.put("body",props);
            docsToReturn.add(docMeta);
        }

        result.put("docs", docsToReturn);
        return result;
    }

    /**
     *
     * @param indexName
     * @param q
     * @return
     */
    public Map<String,Object> search(String indexName, String q) {
        return search(indexName,q, 0, 50);
    }

    /**
     *
     * @param indexName
     * @param document
     * @param operation
     */
    private void indexDocument(String indexName, Map<String,String> document, String operation) {

        // TODO move this out
        String tablePk = "";
        if (indexName.equalsIgnoreCase("movies")) {
            tablePk = "movie_id";
        } else if (indexName.equalsIgnoreCase("actors")) {
            tablePk = "actor_id";
        } else if (indexName.equalsIgnoreCase("courses")) {
            tablePk = "_id";
        }

        if (fulltext) {
            log.info("{} Indexing document ", operation);
            String complexIndexName = SEARCH_INDEX_PREFIX + indexName;
            Client client = searchClients.get(complexIndexName);
            try (Jedis jedis = jedisPool.getResource()) {
                String docId = getKeyForDoc(indexName, document.get(tablePk));
                if (operation.equalsIgnoreCase("CREATE")) {
                    // remove null values
                    // TODO : Check JRedis and manage bug/PR
                    document.values().removeIf(Objects::isNull);
                    // Create/Update the HASH, that will be indexed
                    jedis.hset(docId, document);
                    log.info("Adding fulltext search for {}.", docId);
                } else if (operation.equalsIgnoreCase("UPDATE")) {
                    document.values().removeIf(Objects::isNull);
                    document.keySet().removeIf( e -> e.startsWith("before:")  );
                    jedis.hset(docId, document);
                    log.info("Updating fulltext search for {}.", docId);
                }
            } catch (Exception e) {
                e.printStackTrace();
                log.error("Cannot insert/update document {} .", getKeyForDoc(indexName, document.get(tablePk)));
                log.error(document.toString());
            }
        }
    }

    public Map<String,Object> getInfoIndex(String indexName) {
        String complexIndexName = SEARCH_INDEX_PREFIX + indexName;
        Client client = searchClients.get(complexIndexName);
        return client.getInfo();
    }



    /**
     * Take event and update suggestion for Movie
     * @param data
     * @param operation
     */
    private void updateSuggestForMovie(Map<String, String> data, String operation) {
        processMovie(data, groupNameSuggest , operation, SUGGEST_OP  );
    }

    /**
     * Take event and update suggestion for Movie
     * @param data
     * @param operation
     */
    private void updateSearchForMovie(Map<String, String> data, String operation) {
        processMovie(data, groupNameSearch , operation, INDEX_OP  );
    }

    /**
     *
     * @param data : the Event message
     * @param groupName : the group that is sending the message
     * @param operation : Create, Update, Delete
     * @param ftsActionType : to check if this is suggest or index
     */
    private void processMovie(Map<String, String> data, String groupName, String operation, String  ftsActionType){
        Map<String, String> movie = new HashMap<>();
        movie.put(MoviesSchema.MOVIE_ID, data.get(MoviesSchema.MOVIE_ID));
        movie.put(MoviesSchema.TITLE, data.get(MoviesSchema.TITLE).toString());
        movie.put(MoviesSchema.GENRE , data.get(MoviesSchema.GENRE).toString());
        movie.put(MoviesSchema.VOTES, data.get(MoviesSchema.VOTES));
        movie.put(MoviesSchema.RATING, data.get(MoviesSchema.RATING));
        movie.put(MoviesSchema.RELEASE_YEAR, data.get(MoviesSchema.RELEASE_YEAR));
        movie.put(MoviesSchema.PLOT, data.get(MoviesSchema.PLOT));
        movie.put(MoviesSchema.POSTER, data.get(MoviesSchema.POSTER));

        if (operation.equals("UPDATE")) {
            movie.put("before:"+ MoviesSchema.MOVIE_ID, data.get("before:"+ MoviesSchema.MOVIE_ID));
            movie.put("before:"+ MoviesSchema.TITLE, data.get("before:"+ MoviesSchema.TITLE).toString());
            movie.put("before:"+ MoviesSchema.GENRE , data.get("before:"+ MoviesSchema.GENRE).toString());
            movie.put("before:"+ MoviesSchema.VOTES, data.get("before:"+ MoviesSchema.VOTES));
            movie.put("before:"+ MoviesSchema.RATING, data.get("before:"+ MoviesSchema.RATING));
            movie.put("before:"+ MoviesSchema.RELEASE_YEAR, data.get("before:"+ MoviesSchema.RELEASE_YEAR));
            movie.put("before:"+ MoviesSchema.PLOT, data.get("before:"+ MoviesSchema.PLOT));
            movie.put("before:"+ MoviesSchema.POSTER, data.get("before:"+ MoviesSchema.POSTER));
        }

        // Prepare the indexing call
        String itemType = "movies";
        String newValue = null;
        String oldValue = null;
        String id = movie.get(MoviesSchema.MOVIE_ID).toString();
        if (operation.equals("CREATE")) {
            newValue =  movie.get(MoviesSchema.TITLE).toString();
        } else if (operation.equals("UPDATE")) {
            newValue =  movie.get(MoviesSchema.TITLE).toString();
            oldValue =  movie.get("before:"+ MoviesSchema.TITLE).toString();
        } else  if (operation.equals("DELETE")) {
            oldValue =  movie.get(MoviesSchema.MOVIE_ID).toString();
        }

        if ( ftsActionType.equalsIgnoreCase(INDEX_OP) ) {
            this.indexDocument( itemType, movie, operation);
        }
        if ( ftsActionType.equalsIgnoreCase(SUGGEST_OP)) {
            this.updateAutocomplete(itemType,newValue, oldValue, id);
        }
    }

    /**
     * Take event and update suggestion for Movie
     * @param data
     * @param operation
     */
    private void updateSuggestForActor(Map<String, String> data, String operation) {
        processActor(data, groupNameSuggest , operation, SUGGEST_OP );
    }

    /**
     * Take event and update suggestion for Movie
     * @param data
     * @param operation
     */
    private void updateSearchForActor(Map<String, String> data, String operation) {
        processActor(data, groupNameSearch , operation, INDEX_OP );
    }

    /**
     *
     * @param data
     * @param groupName
     * @param operation
     * @param ftsActionType
     */
    private void processActor(Map<String, String> data, String groupName, String operation, String  ftsActionType){
        Map<String, String> actor = new HashMap<>();
        actor.put("first_name", data.get("first_name"));
        String lastName = data.get("last_name");
        if (lastName == null || lastName.trim().isEmpty()) {
            lastName = "-";
        }
        actor.put("last_name", lastName);
        actor.put("dob", data.get("dob"));
        actor.put("actor_id", data.get("actor_id"));
        actor.put( "full_name",  String.format("%s %s", data.get("first_name"), lastName ));

        if (operation.equals("UPDATE")) {
            String beforeLastName = data.get("before:last_name");
            if (beforeLastName == null || beforeLastName.trim().isEmpty()) {
                beforeLastName = "-";
            }
            actor.put("before:last_name", beforeLastName);
            actor.put("before:dob", data.get("before:dob"));
            actor.put("before:actor_id", data.get("before:actor_id"));
            actor.put( "before:full_name",  String.format("%s %s", data.get("before:first_name"), beforeLastName ));
        }

        // Prepare the indexing call
        String itemType = "actors";
        String newValue = null;
        String oldValue = null;
        String id = actor.get("actor_id").toString();
        if (operation.equals("CREATE")) {
            newValue =  actor.get("full_name").toString();
        } else if (operation.equals("UPDATE")) {
            newValue =  actor.get("full_name").toString();
            oldValue =  actor.get("before:full_name").toString();
        } else  if (operation.equals("DELETE")) {
            oldValue =  actor.get("full_name").toString();
        }

        if ( ftsActionType.equalsIgnoreCase(INDEX_OP) ) {
            this.indexDocument( itemType, actor, operation);
        }
        if ( ftsActionType.equalsIgnoreCase(SUGGEST_OP)) {
            this.updateAutocomplete(itemType, newValue, oldValue, id);
        }
    }

    public Map<String,Object> getMovieByYear(String orderBy, Integer count){
        if (orderBy != null && orderBy.equalsIgnoreCase("year") ) {
            orderBy = "release_year";
        } else {
            orderBy = "sum";
        }
        if (count == null || count == 0){  count = 10; }
        return getMovieStats( "release_year", "Long", orderBy, count );
    }


    public Map<String,Object> getMovieByGenre(String orderBy, Integer count){
        if (orderBy != null && orderBy.equalsIgnoreCase("genre") ) {
            orderBy = "genre";
        } else {
            orderBy = "sum";
        }
        if (count == null || count == 0){  count = 10; }
        return getMovieStats( "genre", "String", orderBy, count );
    }

    /**
     *
     * @param orderBy
     * @return
     */
    public Map<String,Object> getMovieStats(String groupBy, String groupByType, String orderBy, Integer count){
        if (count == null || count == 0){  count = 10; }


        log.info("get getMovieStats {} order by {}", groupBy,  orderBy);
        Map<String,Object> result = new HashMap<>();
        String complexIndexName = SEARCH_INDEX_PREFIX + "movies";
        Client client = searchClients.get(complexIndexName);

        AggregationBuilder aggregation = new AggregationBuilder("*")
                .groupBy("@"+ groupBy, Reducers.count().as("sum"))
                .sortBy(count, SortedField.asc("@"+orderBy));

        long start = System.currentTimeMillis();
        // Execute the aggregate query
        AggregationResult aggRresult = client.aggregate(aggregation);
        long end = System.currentTimeMillis();

        int resultSize = aggRresult.getResults().size();

        result.put("totalResults",aggRresult.totalResults);
        result.put("elaspedTimeMs",end - start);
        result.put("cursorId",aggRresult.getCursorId());
        result.put( "keyLabel", groupBy );
        result.put( "valueLabel", "sum" );
        result.put( "query", aggregation.getArgsString() );
        List<Map<String, Object>> stats = new ArrayList<>();
        for (int i = 0; i <  resultSize  ; i++) {
            Map<String, Object> entry =  new HashMap<>();

            if (groupByType.equalsIgnoreCase("Long")) {
                entry.put("key", Long.toString(aggRresult.getRow(i).getLong(groupBy)));
            } else if (groupByType.equalsIgnoreCase("String")) {
                entry.put("key", aggRresult.getRow(i).getString(groupBy));
            }
            entry.put("value", aggRresult.getRow(i).getLong("sum"));
            stats.add(entry);
        }
        result.put("results", stats);
        return result;
    }

    /**
     * This method use RediSearch aggregation to retrieve the sorted list of genres from movies
     * @return the list of genre as an array
     */
    public Map<String,Object> getAllGenres(){
        log.info("getAllGenres");
        Map<String,Object> result = new HashMap<>();

        String complexIndexName = SEARCH_INDEX_PREFIX + "movies";
        Client client = searchClients.get(complexIndexName);

        AggregationBuilder aggregation = new AggregationBuilder("*")
                .groupBy("@genre")
                .sortBy(100, SortedField.asc("@genre"));

        AggregationResult aggRresult = client.aggregate(aggregation);
        int resultSize = aggRresult.getResults().size();
        String[] genres = new String[resultSize];
        for (int i = 0; i <  resultSize  ; i++) {
            genres[i] = aggRresult.getRow(i).getString("genre");
        }
        result.put("values", genres);
        result.put( "query", aggregation.getArgsString() );
        result.put( "size", resultSize );
        return result;
    }

    /**
     * Get number of movie group by @groupByField
     * @param groupByField
     * @return
     */
    public Map<String,Object> getMovieGroupBy(String groupByField) {
        Map<String,Object> result = new HashMap<>();

        String complexIndexName = SEARCH_INDEX_PREFIX + "movies";
        Client client = searchClients.get(complexIndexName);

        // Create an aggregation query that list the genre
        // FT.AGGREGATE complexIndexName "*" GROUPBY 1 @genre REDUCE COUNT 0 AS nb_of_movies SORTBY 2 @genre ASC
        AggregationBuilder aggregation = new AggregationBuilder()
                .groupBy("@"+groupByField, Reducers.count().as("nb_of_movies"))
                .sortBy( SortedField.asc("@"+groupByField))
                .limit(0,1000); // get all rows

        AggregationResult aggrResult = client.aggregate(aggregation);
        int resultSize = aggrResult.getResults().size();

        List<Map<String, Object>> docsToReturn = new ArrayList<>();
        List<Map<String, Object>> results =  aggrResult.getResults();

        result.put("totalResults",aggrResult.totalResults);

        List<Map<String,Object>> formattedResult = new ArrayList<>();

        // get all result rows and format them
        for (int i = 0; i <  resultSize  ; i++) {
            Map<String, Object> entry =  new HashMap<>();
            entry.put(groupByField, aggrResult.getRow(i).getString(groupByField));
            entry.put("nb_of_movies", aggrResult.getRow(i).getLong("nb_of_movies"));
            formattedResult.add(entry);
        }
        result.put("rows", formattedResult);
        return result;
    }

    /** Execute the search query with
     * some parameter
     * @param queryString
     * @param offset
     * @param limit
     * @param sortBy
     * @return an object with meta: query header and docs: the list of documents
     */
    public Map<String,Object> searchWithPagination(String queryString, int offset, int limit, String sortBy, boolean ascending ){
        // Let's put all the informations in a Map top make it easier to return JSON object
        // no need to have "predefine mapping"
        Map<String,Object> returnValue = new HashMap<>();
        Map<String,Object> resultMeta = new HashMap<>();

        String complexIndexName = SEARCH_INDEX_PREFIX + "movies";
        Client client = searchClients.get(complexIndexName);


        // Create a simple query
        Query query = new Query(queryString)
                .setWithScores()
                .limit(offset, limit);
        // if sort by parameter add it to the query
        if (sortBy != null && !sortBy.isEmpty()) {
            query.setSortBy(sortBy, ascending); // Ascending by default
        }

        long start = System.currentTimeMillis();
        // Execute the query
        SearchResult queryResult = client.search(query);
        long end = System.currentTimeMillis();

        // Adding the query string for information purpose
        resultMeta.put("queryString",queryString);

        // Get the total number of documents and other information for this query:
        resultMeta.put("totalResults", queryResult.totalResults);
        resultMeta.put("elaspedTimeMs",  end - start );
        resultMeta.put("offset", offset);
        resultMeta.put("limit", limit);

        returnValue.put("meta", resultMeta);

        // the docs are returned as an array of document, with the document itself being a list of k/v json documents
        // not the easiest to manipulate
        // the `raw_docs` is used to view the structure
        // the `docs` will contain the list of document that is more developer friendly
        //      capture in  https://github.com/RediSearch/JRediSearch/issues/121
        returnValue.put("raw_docs", queryResult.docs);


        // remove the properties array and create attributes
        List<Map<String, Object>> docsToReturn = new ArrayList<>();
        List<Document> docs =  queryResult.docs;

        for (Document doc :docs) {

            Map<String,Object> props = new HashMap<>();
            Map<String,Object> meta = new HashMap<>();
            meta.put("id", doc.getId());
            meta.put("score", doc.getScore());
            doc.getProperties().forEach( e -> {
                props.put( e.getKey(), e.getValue() );
            });

            Map<String,Object> docMeta = new HashMap<>();
            docMeta.put("meta",meta);
            docMeta.put("fields",props);
            docsToReturn.add(docMeta);
        }

        returnValue.put("docs", docsToReturn);

        return returnValue;
    }

    /**
     * Find movie from Redis database
     * @param movieId
     * @return
     */
    public Map<String,String> getMovieById(String movieId) {
        Map<String,String> result = new HashMap<>();
        try (Jedis jedis = jedisPool.getResource()) {
            result = jedis.hgetAll(DOC_PREFIX_MOVIE + movieId);
        }
        return result;
    }

    /**
     * Find actor from Redis database
     * @param actorId
     * @return
     */
    public Map<String,String> getActorById(String actorId) {
        Map<String,String> result = new HashMap<>();
        try (Jedis jedis = jedisPool.getResource()) {
            result = jedis.hgetAll(DOC_PREFIX_ACTOR + actorId);
        }
        return result;
    }

}
