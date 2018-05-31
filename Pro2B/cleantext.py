#!/usr/bin/env python

"""Clean comment text for easier parsing."""

from __future__ import print_function
from xml.sax import saxutils as su
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

# data = open("comments.txt", "r").read()


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


def build_parse(list):
    result = list
    ######### replace appropriate word for parse comment
    for value in result:
        for index, word in enumerate(value):
            value[index] = value[index].replace("'", "")  # removing all '

    for value in result:
        for index, word in enumerate(value):
            if word.lower() in _CONTRACTIONS:
                value[index] = _CONTRACTIONS[word.lower()]  # mapping to contractions for hes to he's like
    return result


def build_unigram(list):
    ####### replace appropriate word for Unigrams
    n_gram_result = list
    for value in n_gram_result:
        for index, word in enumerate(value):
            value[index] = value[index].replace("'", "")  # removing all '

    for value in n_gram_result:
        for index, word in enumerate(value):
            if word.lower() in _CONTRACTIONS:
                value[index] = _CONTRACTIONS[word.lower()]  # mapping to contractions for hes to he's like
    return n_gram_result


def build_n_gram_list(result):
    temp_list = []
    list_string = []

    for element_list in result:
        for single_element in element_list:

            if (bool(re.search('[\.\,\!\?\;\:]', single_element))):
                # is any sentense-ending or phrase-ending punction is found. Save this chunk as individual element in list_string
                if (re.match('^[a-z].*?[a-z]$', single_element.lower()) is None):

                    if (len(temp_list) > 0):
                        # temp_string =temp_string.rstrip()
                        list_string.append(temp_list)
                        temp_list = []
                else:  # else if it's words like "i'm"  "can't" "abc-abc"
                    # single_element+=single_element+" "
                    temp_list.append(single_element)

            else:
                temp_list.append(single_element)

    if (len(temp_list) > 0):
        #  temp_string =temp_string.rstrip()
        list_string.append(temp_list)

    return list_string


def string_manupulation(plain_text):
    result = []
    temp_list = []

    n_gram_result = []
    n_gram_temp_list = []

    split_lines_list = plain_text.splitlines()

    for comment in split_lines_list:
        newLine_withSpace = comment.replace('\\n', '')  # replace newline with empty
        newLine_withSpace = re.sub(r"http\S+", "", newLine_withSpace)  # replace URL with Empty string
        newLine_withSpace = re.sub(' +', ' ', newLine_withSpace)  # removing mutiple contigous splace in the string
        # newLine_withSpace = re.sub('[^a-zA-Z0-9-.,!?;:\s]+', '', newLine_withSpace)

        n_gram_newLine_withSpace = re.sub('[^a-zA-Z0-9\s\']+[\s\n]+', ' ',
                                          newLine_withSpace)  # remove all punctuation for n_gram

        n_gram_newLine_withSpace = re.sub('[\s\n]+[^a-zA-Z0-9\s\']+', ' ',
                                          newLine_withSpace)  # remove all punctuation for n_gram

        n_gram_newLine_withSpace = re.findall(r"[\w]+[^\s\n]+[\w]+|[\w']+", n_gram_newLine_withSpace)

        n_gram_result.append(n_gram_newLine_withSpace)

        newLine_withSpace = re.findall(r"[\w]+[^\s\n]+[\w]+|[\w']+|[.,!?;:]",
                                       newLine_withSpace)  # Separate all external punctuation such as periods, commas, etc. [.,!?;:]

        result.append(newLine_withSpace)

    # print("#########parse comment:##########")
    # join the words for parse comment
    # print(result)
    parse = build_parse(result)  ####### list containing parse text
    for index, i in enumerate(parse):
        combine = ' '.join(i)
        if (index != len(parse) - 1 and len(combine) > 1 and combine != " "):
            combine = combine.lower() + " "  # lowercase
        else:
            combine = combine.lower()

        temp_list.append(combine)

    # # print each line of string
    # for string in temp_list:
    #     print(string)

    # print("#############unigram###########")
    n_gram_result = build_unigram(n_gram_result)  ####### list containing unigram
    # # join the words for Unigrams
    for index, i in enumerate(n_gram_result):
        combine = ' '.join(i)
        if (index != len(n_gram_result) - 1 and len(combine) > 1 and combine != " "):
            combine = combine.lower() + " "  # lowercase
        else:
            combine = combine.lower()
        n_gram_temp_list.append(combine)

    # # print each line of string
    # for string in n_gram_temp_list:
    #     print(string)

    n_gram_words = build_n_gram_list(result)

    # print("############bigrams###########")
    bigrams = build_bigrams(n_gram_words)  ######### list contaiing bigram
    # print(bigrams)

    # print("############trigrams##############")
    trigrams = build_trigrams(n_gram_words)  ######## list containing trigram
    # print(trigrams)
    final_result = []

    parse_string = ''.join(temp_list)
    unigram_string = ''.join(n_gram_temp_list)
    bigram_string = bigrams
    trigram_string = trigrams

    final_result.append(parse_string)
    final_result.append(unigram_string)
    final_result.append(bigram_string)
    final_result.append(trigram_string)

    return final_result


# You may need to write regular expressions.

def sanitize(text):
    """Do parse the text in variable "text" according to the spec, and return
        a LIST containing FOUR strings
        1. The parsed text.
        2. The unigrams
        3. The bigrams
        4. The trigrams
        """

    # YOUR CODE GOES BELOW:
    return string_manupulation(su.unescape(text))  # returning list of 4 string


if __name__ == "__main__":
    # print(sanitize(data))
    # This is the Python main function.
    # You should be able to run
    # python cleantext.py <filename>
    # and this "main" function will open the file,
    # read it line by line, extract the proper value from the JSON,
    # pass to "sanitize" and print the result as a list.

    # YOUR CODE GOES BELOW.
    if len(sys.argv) == 2:
        with open(sys.argv[1]) as f:
            for line in f:
                data = json.loads(line)
                # sanitize here
                print(sanitize(data['body']))
    else:
        print("[argument wrong!]")

