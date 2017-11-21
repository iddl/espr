Elasticsearch Profile Reader
> Maybe an explain analyze for ES without the x-pack package ? Extremely experimental

Pass the result of an ES query with profiling enabled (https://www.elastic.co/guide/en/elasticsearch/reference/current/search-profile.html#_usage_3)


Example
```
curl -XGET 'localhost:9200/twitter/_search' -d'
{
  "profile": true,
  "query" : {
    "match" : { "message" : "some number" }
  }
} | ./espr.py
'
```

Result
```
Shard: [2aE02wS1R8q_QFnYu6vDVQ][twitter][0]
> BooleanQuery 1873 ms
   > TermQuery 210 ms
   > TermQuery 391 ms
```
