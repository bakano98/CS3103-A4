import spacy
from spacy.lang.en.stop_words import STOP_WORDS
from string import punctuation
from collections import Counter
from heapq import nlargest

nlp = spacy.load("en_core_web_sm")

def summarise(text):
    doc = nlp(text)
    keyword = []
    stopwords = list(STOP_WORDS)
    pos_tag = ['PROPN', 'ADJ', 'NOUN', 'VERB']
    for token in doc:
        if (token.text in stopwords or token.text in punctuation):
            continue
        if (token.pos_ in pos_tag):
            keyword.append(token.text)
    
    freq_word = Counter(keyword)
    max_freq = Counter(keyword).most_common(1)[0][1]
    for word in freq_word.keys():
        freq_word[word] = (freq_word[word]/max_freq)
    
    fw = freq_word.most_common(5)

    sent_strength = {}
    for sent in doc.sents:
        for word in sent:
            if word.text in freq_word.keys():
                if sent in sent_strength.keys():
                    sent_strength[sent] += freq_word[word.text]
                else:
                    sent_strength[sent] = freq_word[word.text]
    
    summarised = nlargest(5, sent_strength, key=sent_strength.get)
    final_sentences = [w.text for w in summarised]
    summed = ''.join(final_sentences)
    return summed