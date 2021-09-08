import pandas as pd
import numpy as np
import string
import nltk
nltk.download('words')
from nltk.corpus import words

DATASET_PATH = "../data/Amharic_Dataset.csv"
JSON_PATH = "../data/data_json.json"

data = pd.read_csv(DATASET_PATH)
data.dropna(inplace = True)
df = data[['category','headline']]

'''remove rows with english words by using nltk corpus'''
Word = list(set(words.words()))
df_final = df[~df['headline'].str.contains('|'.join(Word))]

def remove_punctuations(text):
    for punctuation in string.punctuation:
        text = text.replace(punctuation, '')
    return text

def remove_unwanted(text):
    unwanted_chars = [u'\n', u'\t', u'r', u'\xa0', u'â\x80\x93',u"።",u"፤",u"፣",u"‹",u"›","፡"]
    for char in unwanted_chars:
        text = text.replace(char, '')
    for i in range(10):
        text=text.replace(str(i),'')
        text=text.strip()
    return text

def clean(text):
    text=remove_punctuations(text)
    text=remove_unwanted(text)
    return text

df_final['headline'] = df_final['headline'].apply(clean)
'''save as json, remove lines = True if you want the output to be seperated by commas'''
df_final.to_json(JSON_PATH,orient = "records",lines = True)