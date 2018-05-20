#!/usr/bin/env python

"""Clean comment text for easier parsing."""

from __future__ import print_function

import re
import string
import argparse

__author__ = ""
__email__ = ""

# Some useful data.
_CONTRACTIONS = {
    "tis": "'tis",
    "aint": "ain't",
    "amnt": "amn't",
    "arent": "aren't",
    "cant": "can't",
    "couldve": "could've",
    "couldnt": "couldn't",
    "didnt": "didn't",
    "doesnt": "doesn't",
    "dont": "don't",
    "hadnt": "hadn't",
    "hasnt": "hasn't",
    "havent": "haven't",
    "hed": "he'd",
    "hell": "he'll",
    "hes": "he's",
    "howd": "how'd",
    "howll": "how'll",
    "hows": "how's",
    "id": "i'd",
    "ill": "i'll",
    "im": "i'm",
    "ive": "i've",
    "isnt": "isn't",
    "itd": "it'd",
    "itll": "it'll",
    "its": "it's",
    "mightnt": "mightn't",
    "mightve": "might've",
    "mustnt": "mustn't",
    "mustve": "must've",
    "neednt": "needn't",
    "oclock": "o'clock",
    "ol": "'ol",
    "oughtnt": "oughtn't",
    "shant": "shan't",
    "shed": "she'd",
    "shell": "she'll",
    "shes": "she's",
    "shouldve": "should've",
    "shouldnt": "shouldn't",
    "somebodys": "somebody's",
    "someones": "someone's",
    "somethings": "something's",
    "thatll": "that'll",
    "thats": "that's",
    "thatd": "that'd",
    "thered": "there'd",
    "therere": "there're",
    "theres": "there's",
    "theyd": "they'd",
    "theyll": "they'll",
    "theyre": "they're",
    "theyve": "they've",
    "wasnt": "wasn't",
    "wed": "we'd",
    "wedve": "wed've",
    "well": "we'll",
    "were": "we're",
    "weve": "we've",
    "werent": "weren't",
    "whatd": "what'd",
    "whatll": "what'll",
    "whatre": "what're",
    "whats": "what's",
    "whatve": "what've",
    "whens": "when's",
    "whered": "where'd",
    "wheres": "where's",
    "whereve": "where've",
    "whod": "who'd",
    "whodve": "whod've",
    "wholl": "who'll",
    "whore": "who're",
    "whos": "who's",
    "whove": "who've",
    "whyd": "why'd",
    "whyre": "why're",
    "whys": "why's",
    "wont": "won't",
    "wouldve": "would've",
    "wouldnt": "wouldn't",
    "yall": "y'all",
    "youd": "you'd",
    "youll": "you'll",
    "youre": "you're",
    "youve": "you've"
}


data = open("comments.txt", "r").read()

def string_manupulation(plain_text):
    result = []
    temp_list = []
    
    split_lines_list = plain_text.splitlines()
    for comment in split_lines_list:
        newLine_withSpace = comment.replace('\\n', '')  # replace newline with empty
        newLine_withSpace = re.sub(r"http\S+", "", newLine_withSpace)  # replace URL with Empty string
        newLine_withSpace = re.sub(' +', ' ', newLine_withSpace)  # removing mutiple contigous splace in the string
        newLine_withSpace = re.sub('[^a-zA-Z0-9.,!?;:\s]+', '', newLine_withSpace)  # removing mutiple contigous splace in the string
        newLine_withSpace = re.findall(r"[\w'-]+|[.]|[.,!?;:]",newLine_withSpace)  # Separate all external punctuation such as periods, commas, etc. [.,!?;:]
        
        result.append(newLine_withSpace)
    
    
    # replace appropriate word
    for value in result:
        for index, word in enumerate(value):
            if word.lower() in _CONTRACTIONS:
                value[index]=_CONTRACTIONS[word.lower()]
                print(word)

#join the words
    print(result)
    for i in result:
        combine = ' '.join(i)
        combine = combine.lower()  # lowercase
        temp_list.append(combine)
        
        #print each line of string
        for string in temp_list:
            print(string)


# You may need to write regular expressions.

# def sanitize(text):
#     """Do parse the text in variable "text" according to the spec, and return
#     a LIST containing FOUR strings
#     1. The parsed text.
#     2. The unigrams
#     3. The bigrams
#     4. The trigrams
#     """
#
#     # YOUR CODE GOES BELOW:
#
#     return [parsed_text, unigrams, bigrams, trigrams]


if __name__ == "__main__":
    string_manupulation(data)
# This is the Python main function.
# You should be able to run
# python cleantext.py <filename>
# and this "main" function will open the file,
# read it line by line, extract the proper value from the JSON,
# pass to "sanitize" and print the result as a list.

# YOUR CODE GOES BELOW.
