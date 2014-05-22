twitter-duplicate-detection
===========================

The cleaned version of the experimental code used for our paper Groundhog Day: Duplicate Detection on Twitter

## Model Generation
The duplicate detection needs a model. The model can be trained by 

## Configuration
### Database
The data for experimental purposes were stored in MySQL database. The dump files of table structure can be found in the folder of src/main/resources/sql/.
Please change the nl/wisdelft/twitter/io/twitterdb.properties

## Data Preparation
A lot of features, especially semantic features, depends on precomputed results from a couple of tools. In case you want to use strategies relying on semantic features, you need to run the semantic analysis tools first before duplicate detection can finally work.

### External Resources
The URLs in the tweets can lead us to external resources. For experimental purpose, we crawl them in a batch way. You can have a look at the class of nl.wisdelft.twitter.analytics.redundancy.analyze.ExternalCrawler.java

### Named Entity Recognition
The classes in the package of nl.wisdelft.twitter.analytics.redundancy.analyze .
