QUESTION 1: Take a look at labeled_data.csv. Write the functional dependencies 
implied by the data.

Input_id -> labeldem
Input_id -> labelgop
Input_id -> labeldjt

Any combination of AB -> B
Where A = {Input_id}
B = {labeldem, labelgop, labeldjt}

QUESTION 2: Take a look at the schema for comments. Forget BCNF and 3NF. Does 
the data frame look normalized? In other words, is the data frame free of 
redundancies that might affect insert/update integrity? If not, how would we 
decompose it? Why do you believe the collector of the data stored it in this way?