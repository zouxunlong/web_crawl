from transformers import pipeline
from pymongo import MongoClient
import configuration
import plac
import os


os.chdir(os.path.dirname(__file__))

MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
mongo_client = MongoClient(MONGO_CONNECTION_STRING)

classifier = pipeline("zero-shot-classification",
                      model="facebook/bart-large-mnli")


def topic_classification():

    print('topic classification start.')

    # set candidate classification labels
    candidate_labels = ['Economic', 'Health',
                        'Environmental', 'Societal', 'Attitudes']

    topic_keywords_list = list(mongo_client['pipeline_cna']['testing_set'].distinct(
        'topic_keywords'))

    for topic_keywords in topic_keywords_list:

        # do zero-shot classification to keywords to allocate each topic keywords into five candidat labels
        result = classifier(topic_keywords, candidate_labels, multi_label=True)

        # save the result into mongodb
        mongo_client['pipeline_cna']['topic_classification'].update_one({'_id': topic_keywords}, {'$setOnInsert': {
            'dimension': result['labels'][0], 'dimension_distribution': dict(zip(result['labels'], result['scores']))}}, upsert=True)

        result = mongo_client['pipeline_cna']['testing_set'].update_many({'topic_keywords': topic_keywords}, {
            '$set': {'dimension': result['labels'][0]}})

        print(result.modified_count)
        print(topic_keywords)

    print('topic classification finished.')



def main():
    topic_classification()


if __name__ == "__main__":
    plac.call(main)
