import os
import json
import plac
from pathlib import Path
from sentsplit.segment import SentSplit
import html

def output_metadata(metadata_file_path, metadata):

    with open(metadata_file_path, "w") as metafile:
        metafile.write(json.dumps(metadata))


def extract_sentences(input_path, output_path, lang):

    output_path_dir = os.path.dirname(output_path)
    if not os.path.exists(output_path_dir):
        os.makedirs(output_path_dir)

    if lang in ['en']:
        sent_splitter = SentSplit(lang, strip_spaces=True, maxcut=512)
    if lang in ['zh']:
        sent_splitter = SentSplit(lang, strip_spaces=True, maxcut=150)

    articles_in = 0
    sentences_out = 0

    with open(input_path, encoding="utf8") as fIn, open(output_path, "w", encoding="utf8") as fOut:
        for line in fIn:
            item = json.loads(line)
            if item:
                text = html.unescape(' '.join(item["text"].split()).strip().replace('「','“').replace('」','”'))
                if text:
                    articles_in += 1
                    sentences = sent_splitter.segment(text.strip())
                    if sentences:
                        fOut.write(("\n".join([sentence for sentence in sentences])+"\n"))
                        sentences_out += len(sentences)

    metadata = sent_splitter.config
    metadata['articles_in'] = articles_in
    metadata['sentences_out'] = sentences_out
    metadata_file_path = output_path + '.meta.json'

    output_metadata(metadata_file_path, metadata)


@plac.pos('input_path', "Src File/dir", type=Path)
@plac.pos('output_path', "Tgt File/dir", type=Path)
@plac.pos('lang', "language type", type=str)
def main(input_path="/home/xuancong/airflow/data/zh_newsmarket/zh_newsmarket.jl", output_path="/home/xuancong/airflow/data/zh_newsmarket/zh_newsmarket.sent", lang='zh'):

    os.chdir(os.path.dirname(__file__))

    if os.path.isfile(input_path):
        if lang in {'ms','en','id','vi','ta','th'}:
            lang = 'en'
        elif lang in {'zh'}:
            lang = 'zh'
        extract_sentences(str(input_path), str(output_path), lang)

    elif os.path.isdir(input_path):
        for rootdir, dirs, files in os.walk(input_path):
            if rootdir.split('_')[-1] in {'ms','en','id','vi','ta','th'}:
                lang = 'en'
            elif rootdir.split('_')[-1] in {'zh'}:
                lang = 'zh'
            for file in files:
                if file.endswith('.jl'):
                    input_file = os.path.join(rootdir, file)
                    output_file = os.path.join(rootdir.replace(str(input_path), str(output_path)), file.replace('.jl', '.sent'))
                    extract_sentences(input_file, output_file, lang)
                    print("finished {}".format(input_file), flush=True)
    else:
        print("invalid input_file")


if __name__ == "__main__":
    plac.call(main)
    print('finished all')
