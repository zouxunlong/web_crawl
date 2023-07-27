import os
import json
import plac
from pathlib import Path
from easynmt import EasyNMT


model = EasyNMT('m2m_100_418M')


def output_metadata(metadata_file_path, metadata):

    with open(metadata_file_path, "w") as metafile:
        metafile.write(json.dumps(metadata))


def translate_sentences(sentences_src, output_file, src, lang):

    sentences_tgt = model.translate(
        sentences_src, target_lang=lang, source_lang=src)

    assert len(sentences_src) == len(
        sentences_tgt), "source sentences is not matching target sentences"

    with open(os.path.splitext(output_file)[0] + '.m2m_100_418M.' + src + '-' + lang, "w", encoding="utf8") as fOut:
        fOut.write("\n".join([r" | ".join(tuple) for tuple in zip(sentences_src, sentences_tgt) if len(tuple[0])>0 and len(tuple[1])>0 ]))

    return len(sentences_tgt)


def translation(input_file, output_file, src, dest):

    output_file_dir = os.path.dirname(output_file)
    if not os.path.exists(output_file_dir):
        os.makedirs(output_file_dir)

    metadata = {}
    metadata['translation_engine'] = 'm2m_100_418M'

    sentences_src = [line.strip() for line in open(
            input_file, encoding="utf8").readlines()]

    metadata['sentences_src'] = len(sentences_src)

    if isinstance(dest, str):
        lang = dest
        metadata['sentences_tgt_' + lang] = translate_sentences(sentences_src, output_file, src, lang)
    elif isinstance(dest, list):
        for lang in dest:
            metadata['sentences_tgt_' + lang] = translate_sentences(sentences_src, output_file, src, lang)

    metadata_file_path = os.path.splitext(
        output_file)[0] + '.m2m_100_418M.meta.json'

    output_metadata(metadata_file_path, metadata)


@ plac.pos('input_path', "Src File/dir", type=Path)
@ plac.pos('output_path', "Tgt File/dir", type=Path)
@ plac.pos('src', "source language", type=str)
@ plac.pos('dest', "destiny language", type=str)
def main(input_path="/home/zxl/airflow/data/stage3", output_path="/home/zxl/airflow/data/stage4", src='en', dest=['zh', 'ms', 'ta', 'en']):

    os.chdir(os.path.dirname(__file__))

    if os.path.isfile(input_path):
        input_file = str(input_path)
        output_file = str(output_path)
        translation(input_file, output_file, src, dest)

    elif os.path.isdir(input_path):
        for rootdir, dirs, files in os.walk(input_path):
            if rootdir.split('_')[-1] == 'ms':
                src = 'ms'
            else:
                src = 'en'
            for file in files:
                if file.endswith('.sent'):
                    input_file = os.path.join(rootdir, file)
                    output_file = os.path.join(rootdir.replace(str(input_path), str(output_path)), file.replace('.sent', '.lang'))
                    translation(input_file, output_file, src, dest)

    else:
        print("invalid input_file")


if __name__ == "__main__":
    plac.call(main)
