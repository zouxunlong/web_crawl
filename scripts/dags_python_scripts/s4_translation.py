import os
import json
import plac
from pathlib import Path


def output_metadata(metadata_file_path, metadata):

    with open(metadata_file_path, "w") as metafile:
        metafile.write(json.dumps(metadata))


def translate_sentences(sentences_src, output_file, src, lang):

    if lang == src:
        sentences_tgt = [sentence for sentence in sentences_src]
    elif lang == 'zh':
        sentences_tgt = ['这是测试翻译。' for sentence in sentences_src]
    elif lang=='en':
        sentences_tgt = ['This is the test translation.' for sentence in sentences_src]
    elif lang=='ms':
        sentences_tgt = ['Ini adalah terjemahan ujian.' for sentence in sentences_src]
    elif lang=='ta':
        sentences_tgt = ['இது சோதனை மொழிபெயர்ப்பு.' for sentence in sentences_src]
    elif lang=='vi':
        sentences_tgt = ['Đây là bản dịch thử nghiệm.' for sentence in sentences_src]
    elif lang=='id':
        sentences_tgt = ['Ini adalah terjemahan tes.' for sentence in sentences_src]
    else:
        raise ValueError("Sorry, no language type matched.")

    assert len(sentences_src) == len(sentences_tgt), 'length of src and target do not match'

    with open(os.path.splitext(output_file)[0] + '.pseudo.' + lang, "w", encoding="utf8") as fOut:
        fOut.write("\n".join([sentence_tgt for sentence_tgt in sentences_tgt]))

    return len(sentences_tgt)


def translation(input_file, output_file, src, dest):

    output_file_dir = os.path.dirname(output_file)
    if not os.path.exists(output_file_dir):
        os.makedirs(output_file_dir)

    metadata = {}
    metadata['translation_engine'] = 'pseudo'

    sentences_src = [line.strip() for line in open(
        input_file, encoding="utf8").readlines()]

    metadata['sentences_src'] = len(sentences_src)

    if isinstance(dest, str):
        lang = dest
        metadata['sentences_tgt_' +
                 lang] = translate_sentences(sentences_src, output_file, src, lang)
    elif isinstance(dest, list):
        for lang in dest:
            metadata['sentences_tgt_' +
                     lang] = translate_sentences(sentences_src, output_file, src, lang)

    metadata_file_path = os.path.splitext(
        output_file)[0] + '.pseudo.meta.json'

    output_metadata(metadata_file_path, metadata)


@ plac.pos('input_path', "Src File/dir", type=Path)
@ plac.pos('output_path', "Tgt File/dir", type=Path)
@ plac.pos('src', "source language", type=str)
@ plac.pos('dest', "destiny language", type=str)
def main(input_path="/home/zxl/airflow/data/stage3", output_path="/home/zxl/airflow/data/stage4", src='en', dest=['zh', 'ms', 'ta', 'en','vi','id']):

    os.chdir(os.path.dirname(__file__))

    if os.path.isfile(input_path):
        input_file = str(input_path)
        output_file = str(output_path)
        translation(input_file, output_file, src, dest)

    elif os.path.isdir(input_path):
        for rootdir, dirs, files in os.walk(input_path):
            for file in files:
                if file.endswith('.sent'):
                    input_file = os.path.join(rootdir, file)
                    output_file = os.path.join(rootdir.replace(
                        str(input_path), str(output_path)), file.replace('.sent', '.lang'))
                    src=rootdir[-2:]
                    translation(input_file, output_file, src, dest)

    else:
        print("invalid input_file")


if __name__ == "__main__":
    plac.call(main)
