import itertools
import json
import os
from pathlib import Path
import socket
import requests
import plac
from concurrent.futures import ThreadPoolExecutor

class Back_translator:

    def __init__(self,
                 batch_size=50,
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
            response = requests.post(url, json={'text': '\n'.join(batch_sentences_src)})
            sentences_tgt.extend(response.json()['translations'])
        assert len(sentences_src) == len(sentences_tgt), 'length of src and target do not match'

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
                    batch_sentences_tgt += s.recv(8192).decode('utf-8')
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
        with open(output_file, "w", encoding="utf8") as f_out:
            for i in range(0, len(sentences_src), 10000):
                batch_sentences_src = sentences_src[i:i+10000]
                batch_sentences_tgt = translator.translate(batch_sentences_src, src, tgt)
                assert len(batch_sentences_tgt) == len(batch_sentences_src), 'length of src and target do not match'
                for i in range(len(batch_sentences_src)):
                    f_out.write("{} ||| {}\n".format(batch_sentences_src[i].replace("|", " "), batch_sentences_tgt[i].replace("|", " ")))
    print('finished {}'.format(input_file), flush=True)


@ plac.pos('src', "source language", type=str)
@ plac.pos('tgt', "target language", type=str)
@ plac.pos('worker', "worker number", type=int)
@ plac.pos('input_path', "Src File/dir", type=Path)
def main(src='th',
         tgt='en',
         worker=4,
         input_dir="/home/xuanlong/web_crawl/data/news_article",
         ):
    
    print(os.getpid(), flush=True)

    os.chdir(os.path.dirname(__file__))

    with ThreadPoolExecutor(max_workers=worker) as pool:

        for rootdir, dirs, files in os.walk(input_dir):

            src_lang = os.path.basename(rootdir)[:2]
            if src_lang not in [src]:
                continue
            files.sort(reverse=True)

            for file in files:

                input_file = os.path.join(rootdir, file)
                output_file = os.path.join(rootdir, file.replace('jsonl', src+'2'+tgt))
                pool.submit(translation, input_file, output_file, src, tgt)

                print('task for {} submitted.....'.format(input_file), flush=True)

    print('ThreadPool closed.....', flush=True)


if __name__ == "__main__":
    # batch_sentences_tgt = translator.translate(["แม่ขอโทษลูกด้วย"], "th", "en")
    # print(translator.translate(["this is testing & this is another testing."], "en", "zh"), flush=True)
    # print(translator.translate(["how are you? https://youtu.be/XQFuMwDFO48"], "en", "zh"), flush=True)
    # print(translator.translate(["how are you? https://youtu.be/XQFuMwDFO48"], "en", "ms"), flush=True)
    # print(translator.translate(["பேஸ்புக்கில் சிறுகதை ஒன்றை பதிவிட்டதன் மூலம் பௌத்த பிக்குமாரை தனது எழுத்துக்களால் விமர்சித்தாரென சிவில் மற்றும் அரசியல் உரிமைகள் பற்றிய ஐ.நா. ஆதரவு சர்வதேச உடன்படிக்கையின் (ICCPR) கீழ் ஏப்ரல் மாதம் முதலாம் திகதி அவர் கைதுசெய்யப்பட்டார்."], "ta", "en"), flush=True)
    # print(translator.translate(["你好吗？"], "zh", "en"), flush=True)
    print(translator.translate(["Sebanyak 94.6 peratus jumlah perdagangan dihantar melalui pengangkutan laut. Ini diikuti oleh pengangkutan udara (3.6%) dan melalui darat (1.8%)."], "ms", "en"), flush=True)

