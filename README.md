Knowledge base for Word Prediction
knowledge-base for Hebrew word-prediction system, based on Google 3-Gram Hebrew dataset, using Amazon Elastic Map-Reduce (EMR).

Introduction
We will generate a knowledge-base for Hebrew word-prediction system, based on Google 3-Gram Hebrew dataset, using Amazon Elastic Map-Reduce (EMR). The produced knowledge-base indicates for a each pair of words the probability of their possible next words. In addition, We will examine the quality of the algorithm according to statistic measures and manual analysis.

Goal
Our goal is to build a map-reduce system for calculating the probability of each trigram (w1,w2,w3) found in a given corpus, to run it on the Amazon Elastic MapReduce service, and to generate the output knowledge base with the resulted probabilities.
The input corpus is the Hebrew 3-Gram dataset of Google Books Ngrams.

The output of the system is a list of word trigrams (w1,w2,w3) and their probabilities (P(w1w2w3))). 
For example:

קפה נמס עלית 0.6
קפה נמס מגורען 0.4

קפה שחור חזק 0.6

קפה שחור טעים 0.3

קפה שחור חם 0.1

…

שולחן עבודה ירוק 0.7

שולחן עבודה מעץ 0.3

…
