import os
import time
import plac
from pathlib import Path
import requests
import threading
import socket


class Back_translator_sgtt:

    def __init__(self,
                 batch_size=10,
                 en2th_api='http://10.2.56.190:5001/en2th',
                 th2en_api='http://10.2.56.190:5001/th2en',
                 sgtt_api='http://10.2.56.190:5008/translator'
                 ):
        self.batch_size = batch_size
        self.urls = {'en2th': en2th_api,
                     'th2en': th2en_api,
                     'en2zh': sgtt_api,
                     'en2ms': sgtt_api,
                     'en2ta': sgtt_api,
                     'zh2en': sgtt_api,
                     'ms2en': sgtt_api,
                     'ta2en': sgtt_api,
                     }

    def translate_th(self, sentences_src, src, tgt):

        url = self.urls[src+'2'+tgt]
        sentences_tgt = []

        for i in range(0, len(sentences_src), self.batch_size):
            batch_sentences_src = sentences_src[i:i+self.batch_size]
            response = requests.post(
                url, json={'text': '\n'.join(batch_sentences_src)})
            sentences_tgt.extend(response.json()['translations'])

        assert len(sentences_src) == len(
            sentences_tgt), 'length of src and target do not match'

        return sentences_tgt

    def translate_sgtt(self, sentences_src, src, tgt):
        url = self.urls[src+'2'+tgt]
        sentences_tgt = []
        source=src+"_SG"
        target=tgt+"_SG"

        for i in range(0, len(sentences_src), self.batch_size):
            batch_sentences_src = sentences_src[i:i+self.batch_size]
            response = requests.post(
                url, json={"source":source, "target":target,"query": '\n'.join(batch_sentences_src)})
            batch_sentences_tgt=[item["translatedText"] for item in response.json()["data"]["translations"]]
            sentences_tgt.extend(batch_sentences_tgt)

        assert len(sentences_src) == len(
            sentences_tgt), 'length of source and target do not match'

        return sentences_tgt


    def translate(self, sentences_src, src, tgt):

        if (src, tgt) in [('en', 'th'), ('th', 'en')]:
            return self.translate_th(sentences_src, src, tgt)

        if (src, tgt) in [('en', 'zh'), ('zh', 'en'), ('en', 'ms'), ('ms', 'en'), ('en', 'ta'), ('ta', 'en')]:
            return self.translate_sgtt(sentences_src, src, tgt)


translator = Back_translator_sgtt()


def translate_sentences(sentences_src, output_file, src, lang):

    sentences_tgt = []

    sentences_tgt=translator.translate(sentences_src, src, lang)

    assert len(sentences_src) == len(
        sentences_tgt), 'length of src and target do not match'

    sentence_pairs = zip(sentences_src, sentences_tgt)

    with open(output_file, "w", encoding="utf8") as f_out:
        for sentence_pair in sentence_pairs:
            f_out.write("{} ||| {}\n".format(sentence_pair[0].replace("|", " "),
                                             sentence_pair[1].replace("|", " ")))


def translation(input_file, output_path, src, dest):

    output_path_dir = os.path.dirname(output_path)
    if not os.path.exists(output_path_dir):
        try:
            os.makedirs(output_path_dir)
        except:
            print("dir exists")
            pass

    sentences_src = [line.strip() for line in open(
        input_file, encoding="utf8").readlines()]

    for lang in dest:
        output_file = os.path.splitext(output_path)[0] + '.i2r.' + src+'2'+lang
        if not os.path.exists(output_file):
            translate_sentences(sentences_src, output_file, src, lang)
    
    print('finished {}'.format(input_file), flush=True)


@ plac.pos('input_path', "Src File/dir", type=Path)
@ plac.pos('output_path', "Tgt File/dir", type=Path)
@ plac.pos('src', "source language", type=str)
def main(input_path="/home/xuancong/airflow/data/stage3",
         output_path="/home/xuancong/airflow/data/stage4",
         src='ms'):

    os.chdir(os.path.dirname(__file__))

    threads = []
    for rootdir, dirs, files in os.walk(input_path):
        src = rootdir[-2:]
        files.sort(reverse=True)
        for file in files:
            if file.endswith('.sent'):
                input_file = os.path.join(rootdir, file)
                output_file = os.path.join(rootdir.replace(str(input_path), str(output_path)), file)

                if src in ['ms']:
                    threads.append(threading.Thread(target=translation, args=(
                        input_file, output_file, src, ['en'])))
                    print('threads appended {}'.format(input_file), flush=True)

    for index, thread_chunk in enumerate([threads[i:i+10] for i in range(0, len(threads), 10)]):
        start_time=time.time()
        print("--- {:.0f} seconds ---".format(time.time() - start_time), flush=True)
        for thread in thread_chunk:
            thread.start()
        for thread in thread_chunk:
            thread.join()
        print("finished thread_chunk {} in {:.0f} seconds".format(str(index), time.time() - start_time), flush=True)


if __name__ == "__main__":
    plac.call(main)
    print("finished all", flush=True)
