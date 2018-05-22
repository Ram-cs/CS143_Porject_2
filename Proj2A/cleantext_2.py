#!/usr/bin/env python

"""Clean comment text for easier parsing."""

from __future__ import print_function

import bz2
import re
import string
import argparse
import sys
import json

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

# reference : http://locallyoptimal.com/blog/2013/01/20/elegant-n-gram-generation-in-python/
def bigram_helper(input_list):
    bigram_list = []
    for i in range(len(input_list) - 1):
        bigram_list.append((input_list[i], input_list[i + 1]))
    return bigram_list

def build_bigrams(result):
    return_list = []
    for sentence in result:  # a sentense is like ['hello','world']
        tuple_list = bigram_helper(
            sentence)  # a tuple_list is like: [('hello','world'),('hello','world'),('hello','world)]
        for tuple in tuple_list:
            return_list.append(tuple[0] + "_" + tuple[1])  # ('hello','world) becomes ['hello_world']

    joined_sentence = ' '.join(return_list)
    return joined_sentence.lower()


def trigram_helper(input_list):
    trigram_list = []
    for i in range(len(input_list) - 2):
        trigram_list.append((input_list[i], input_list[i + 1], input_list[i + 2]))
    return trigram_list

def build_trigrams(result):
    return_list = []
    for sentence in result:
        tuple_list = trigram_helper(sentence)
        for tuple in tuple_list:
            return_list.append(tuple[0] + "_" + tuple[1] + "_" + tuple[2])

    joined_sentence = ' '.join(return_list)
    return joined_sentence.lower()


def build_parser(list):
    temp_list = []
    return_parser = []
    for i in list:
        combine = ' '.join(i)
        combine = combine.lower()  # lowercase
        temp_list.append(combine)

    return temp_list

def build_unigram(unigram_result):
    # join the words for Unigrams
    unigram_temp_list = []
    for i in unigram_result:
        combine = ' '.join(i)
        combine = combine.lower()  # lowercase
        unigram_temp_list.append(combine)

    return unigram_temp_list

def string_manupulation(plain_text):
    result = []
    n_gram_result = []

    unigram_result = []

    split_lines_list = plain_text.splitlines()
    for comment in split_lines_list:
        newLine_withSpace = comment.replace('\\n', '')  # replace newline with empty
        newLine_withSpace = re.sub(r"http\S+", "", newLine_withSpace)  # replace URL with Empty string
        newLine_withSpace = re.sub(' +', ' ', newLine_withSpace)  # removing mutiple contigous splace in the string

        n_gram_newLine_withSpace = re.sub(r'([\s]*[\w]*([^a-zA-Z0-9\'\s\-]|([\'\-]{2,}))+[\w]*)+[\s]*', ' ',newLine_withSpace)  # remove anything like abc? .abc ab..c ab..c..a, but not ab'c or ab-c that has one ' or one - on it.
        newLine_withSpace = re.sub('[^a-zA-Z0-9.,!?;:\s]+', '', newLine_withSpace)

        unigram_newLine_withSpace = re.sub('[^a-zA-Z0-9\s]+', '', newLine_withSpace)

        n_gram_newLine_withSpace = re.sub('[^a-zA-Z0-9\s]+', '', n_gram_newLine_withSpace)

        n_gram_newLine_withSpace = re.findall(r"[\w'-]+|[.]", n_gram_newLine_withSpace)
        n_gram_result.append(n_gram_newLine_withSpace)

        unigram_newLine_withSpace = re.findall(r"[\w'-]+|[.]", unigram_newLine_withSpace)
        unigram_result.append(unigram_newLine_withSpace)

        newLine_withSpace = re.findall(r"[\w'-]+|[.]|[.,!?;:]",
                                       newLine_withSpace)  # Separate all external punctuation such as periods, commas, etc. [.,!?;:]

        result.append(newLine_withSpace)

    # replace appropriate word for parse comment
    for value in result:
        for index, word in enumerate(value):
            if word.lower() in _CONTRACTIONS:
                value[index] = _CONTRACTIONS[word.lower()]

    # replace appropriate word for Unigrams
    for value in unigram_result:
        for index, word in enumerate(value):
            if word.lower() in _CONTRACTIONS:
                value[index] = _CONTRACTIONS[word.lower()]
    # replace appropriate word for bigram and trigram
    for value in n_gram_result:
        for index, word in enumerate(value):
            if word.lower() in _CONTRACTIONS:
                value[index] = _CONTRACTIONS[word.lower()]

    print("************** PARSE ***********************:")
    # print each line of string
    parse = build_parser(result)
    for lines in parse:
        print(lines)


    print("************** UIGRAM ***********************:")
    #print each line of unigram from list
    unigram_result_list = build_unigram(unigram_result)
    for string in unigram_result_list:
        print(string)

    # for i in n_gram_result:
    #     combine = ' '.join(i)
    #     combine = combine.lower()  # lowercase
    #     n_gram_temp_list.append(combine)


    print("************** BIGRAME ***********************:")
    bigrams = build_bigrams(n_gram_result)
    print(bigrams)

    print("************** TRIGRAME ***********************:")
    trigrams = build_trigrams(n_gram_result)
    print(trigrams)

# Example Comment:
#
# I'm afraid I can't explain myself, sir. Because I am not myself, you see?
#


# Parsed Comment (Returned String 1):
#
# i'm afraid i can't explain myself , sir . because i am not myself , you see ?
#


# Unigrams (Returned String 2):
#
# i'm afraid i can't explain myself sir because i am not myself you see
#



# Bigrams (Returned String 3):
#
# i'm_afraid afraid_i i_can't can't_explain explain_myself because_i i_am am_not not_myself you_see
#


# Trigrams (Returned String 4):
#
# i'm_afraid_i afraid_i_can't i_can't_explain can't_explain_myself because_i_am i_am_not am_not_myself



# You may need to write regular expressions.
#
# def sanitize(text):
#     """Do parse the text in variable "text" according to the spec, and return
#         a LIST containing FOUR strings
#         1. The parsed text.
#         2. The unigrams
#         3. The bigrams
#         4. The trigrams
#         """
#
#     # YOUR CODE GOES BELOW:
#     string_manupulation(text)
#     return [parsed_text, unigrams, bigrams, trigrams]


if __name__ == "__main__":
    string_manupulation(data)
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.
    # with open(sys.argv[1]) as f:
    #     for line in f:
    #         data = json.loads(line)
    #         # process comment here
    #         sanitize(data['body'])
# print(data['body'])


# YOUR CODE GOES BELOW.

