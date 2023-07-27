import itertools
import json
import os
from pathlib import Path
import socket
import threading
import time
import requests
import plac
from concurrent.futures import ThreadPoolExecutor

class Back_translator:

    def __init__(self,
                 batch_size=10,
                 en2th_api='http://10.2.56.190:5001/en2th',
                 th2en_api='http://10.2.56.190:5001/th2en',
                 sgtt_api='http://10.2.56.190:5008/translator',
                 en2vi_port=25602,
                 vi2en_port=25603,
                 host='10.2.56.41'
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
        self.ports = {
                      'en2vi': en2vi_port,
                      'vi2en': vi2en_port,
                      }
        self.host = host

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

    def translate_vi(self, sentences_src, src, tgt):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            host = self.host
            port = self.ports[src+'2'+tgt]
            s.connect((host, port))
            sentences_tgt = []

            for i in range(0, len(sentences_src), self.batch_size):
                batch_sentences_src = sentences_src[i:i+self.batch_size]
                batch_sentences_tgt = ""
                s.sendall(bytes('\n'.join(batch_sentences_src)+'\n', encoding='utf8'))
                while batch_sentences_tgt.count("\n") != len(batch_sentences_src):
                    batch_sentences_tgt += s.recv(4096).decode('UTF-8')
                batch_sentences_tgt = batch_sentences_tgt.strip().split('\n')
                sentences_tgt.extend(batch_sentences_tgt)

        assert len(sentences_src) == len(sentences_tgt), 'length of src and target do not match'
        return sentences_tgt

    def translate(self, sentences_src, src, tgt):

        if (src, tgt) in [('en', 'th'), ('th', 'en')]:
            return self.translate_th(sentences_src, src, tgt)

        if (src, tgt) in [('en', 'vi'), ('vi', 'en')]:
            return self.translate_vi(sentences_src, src, tgt)

        if (src, tgt) in [('en', 'zh'), ('zh', 'en'), ('en', 'ms'), ('ms', 'en'), ('en', 'ta'), ('ta', 'en')]:
            return self.translate_sgtt(sentences_src, src, tgt)

translator = Back_translator()

def translation(input_file, output_file, src, tgt):

    sentences_src = list(itertools.chain.from_iterable([json.loads(line)['split_sentences'] for line in open(input_file, encoding="utf8").readlines()]))

    if not os.path.exists(output_file):

        sentences_tgt = translator.translate(sentences_src, src, tgt)
        assert len(sentences_src) == len(sentences_tgt), 'length of src and target do not match'

        with open(output_file, "w", encoding="utf8") as f_out:
            for i in len(sentences_src):
                f_out.write("{} ||| {}\n".format(sentences_src[i].replace("|", " "), sentences_tgt[i].replace("|", " ")))

    print('finished {}'.format(input_file), flush=True)


@ plac.pos('src', "source language", type=str)
@ plac.pos('tgt', "target language", type=str)
@ plac.pos('input_path', "Src File/dir", type=Path)
def main(src,
         tgt,
         input_dir="/home/xuanlong/web_crawl/data/news_article",
         ):

    os.chdir(os.path.dirname(__file__))

    with ThreadPoolExecutor(max_workers=10) as pool:

        for rootdir, dirs, files in os.walk(input_dir):

            src_lang = os.path.basename(rootdir)[:2]
            if src_lang not in [src]:
                continue
            files.sort()

            for file in files:

                input_file = os.path.join(rootdir, file)
                output_file = os.path.join(rootdir, file.replace('jsonl',src+'2'+tgt))
                pool.submit(translation, input_file, output_file, src, tgt)

                print('task submitted.....', flush=True)

    print('ThreadPool closed.....', flush=True)


if __name__ == "__main__":
    plac.call(main)
    print("finished all", flush=True)


