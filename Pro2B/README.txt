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