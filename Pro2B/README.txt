QUESTION 1: Take a look at labeled_data.csv. Write the functional dependencies
implied by the data.

Input_id -> labeldem
Input_id -> labelgop
Input_id -> labeldjt

And other trivial combinations

Any combination of AB -> B
Where A = {Input_id}
B = {labeldem, labelgop, labeldjt}

QUESTION 2: Take a look at the schema for comments. Forget BCNF and 3NF. Does
the data frame look normalized? In other words, is the data frame free of
redundancies that might affect insert/update integrity? If not, how would we
decompose it? Why do you believe the collector of the data stored it in this way?

It is not normalized. For example author + author_flair_text creates
redundancies as does subreddit_id + subreddit. To decompose:

Relations with:
{subreddit_id, subreddit}
{author, can_guild, link_id, author_flair_text, author_flair_css_class,
author_cakeday}
{score, controversiality}
{id, author, subreddit_id, stickied, score, retrieved_on, permalink, parent_id,
is_submitter, gilded, edited, created_utc, body}

It was possibly organized this way because the collector wanted everything in
one relation for convenience sake.

QUESTION 3:
This is my input for explain():
"SELECT data_table.Input_id, data_table.labeldem, data_table.labelgop, data_table.labeldjt, comment_table.body as
 comment_body FROM data_table JOIN comment_table ON data_table.Input_id = comment_table.id")

 This is the output of explain():
 == Physical Plan ==
 *(2) Project [Input_id#170, labeldem#171, labelgop#172, labeldjt#173, body#4 AS comment_body#178]
 +- *(2) BroadcastHashJoin [Input_id#170], [id#14], Inner, BuildLeft
    :- BroadcastExchange HashedRelationBroadcastMode(List(input[0, string, true]))
    :  +- *(1) Project [Input_id#170, labeldem#171, labelgop#172, labeldjt#173]
    :     +- *(1) Filter isnotnull(Input_id#170)
    :        +- *(1) FileScan parquet [Input_id#170,labeldem#171,labelgop#172,labeldjt#173] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/media/sf_vm-shared/2p/Pro2B/labeled_data], PartitionFilters: [], PushedFilters: [IsNotNull(Input_id)], ReadSchema: struct<Input_id:string,labeldem:string,labelgop:string,labeldjt:string>
    +- *(2) Project [body#4, id#14]
       +- *(2) Filter isnotnull(id#14)
          +- *(2) FileScan parquet [body#4,id#14] Batched: true, Format: Parquet, Location: InMemoryFileIndex[file:/media/sf_vm-shared/2p/Pro2B/comments], PartitionFilters: [], PushedFilters: [IsNotNull(id)], ReadSchema: struct<body:string,id:string>



 The result of .explain() shows that even though we specific JOIN in our sql query, a underlined BroadcastHashJoin will be run.
 After reads in all the parquet files, spark will filters out all the rows that is NULL on comment_table.id and all the rows that is NULL on data_table.Input_id. When one of the relation is small enough to fit in memory that is less than the autoBroadcastJoinThreashold (i.e. the data_table relation), spark performs a broadcast hash join. The smaller relation data_table will only be read once and it will be used to create hashtable with key be Input_id and values are all the attributes. The smaller relation will joined with the larger relation in the Mapper phase by using the hashtable on comment_table.id. Hashing comment_table.id will give us the key which will map to a value from data_table. This joining operations is fast because we are using the hash table. While we need to cached only the hash table, the hash table should still be relatively small (<autoBroadcastJoinThreashold).
