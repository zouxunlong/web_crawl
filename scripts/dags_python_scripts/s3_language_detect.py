import os
import plac
from pathlib import Path
import sys
import pycld2 as cld2
import cld3
import re


pattern_punctuation = r"""[!?,.．:;"「」─α °#$£€%&'()+-/<≤=≠≥>@[\]^_{|}，。、—‘’“”：；【】￥…《》？！（）]"""
pattern_url = r"[(http(s)?):\/\/(www\.)?a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)"
pattern_email = r"[\w\-\.]+@([\w\-]+\.)+[\w\-]{2,4}"
pattern_arabic = r"[\u0600-\u06FF]"
pattern_chinese = r"[\u4e00-\u9fff]"
pattern_tamil = r"[\u0B80-\u0BFF]"
pattern_thai = r"[\u0E00-\u0E7F]"
pattern_russian = r"[\u0400-\u04FF]"
pattern_korean = r"[\uac00-\ud7a3]"
pattern_japanese = r"[\u3040-\u30ff\u31f0-\u31ff]"
pattern_vietnamese = r"[àáãạảăắằẳẵặâấầẩẫậèéẹẻẽêềếểễệđìíĩỉịòóõọỏôốồổỗộơớờởỡợùúũụủưứừửữựỳỵỷỹýÀÁÃẠẢĂẮẰẲẴẶÂẤẦẨẪẬÈÉẸẺẼÊỀẾỂỄỆĐÌÍĨỈỊÒÓÕỌỎÔỐỒỔỖỘƠỚỜỞỠỢÙÚŨỤỦƯỨỪỬỮỰỲỴỶỸÝ]"
pattern_emoji = r'[\U0001F1E0-\U0001F1FF\U0001F300-\U0001F64F\U0001F680-\U0001FAFF\U00002702-\U000027B0]'


def lang_detect(text_for_lang_detect):

    lang_detected = set()

    text_for_lang_detect = ' '.join(re.sub(r"{}|{}|{}".format(
        pattern_url,
        pattern_email,
        pattern_punctuation
    ), " ", text_for_lang_detect, 0, re.I).split()).strip().lower()

    if text_for_lang_detect:
        if re.search(pattern_arabic, text_for_lang_detect):
            lang_detected.add('ar')
        if re.search(pattern_chinese, text_for_lang_detect):
            lang_detected.add('zh')
        if re.search(pattern_tamil, text_for_lang_detect):
            lang_detected.add('ta')
        if re.search(pattern_russian, text_for_lang_detect):
            lang_detected.add('ru')
        if re.search(pattern_korean, text_for_lang_detect):
            lang_detected.add('ko')
        if re.search(pattern_japanese, text_for_lang_detect):
            lang_detected.add('ja')
        if re.search(pattern_vietnamese, text_for_lang_detect):
            lang_detected.add('vi')

    return lang_detected


def unwanted_character_filtered(text_for_filter):
    text = re.sub(r'[^a-zA-Z0-9\s\t{}{}{}{}{}{}{}{}{}{}]'.format(pattern_punctuation[1:-1],
                                                                 pattern_arabic[1:-1],
                                                                 pattern_chinese[1:-1],
                                                                 pattern_tamil[1:-1],
                                                                 pattern_thai[1:-1],
                                                                 pattern_russian[1:-1],
                                                                 pattern_korean[1:-1],
                                                                 pattern_japanese[1:-1],
                                                                 pattern_vietnamese[1:-1],
                                                                 pattern_emoji[1:-1],
                                                                 ), '', text_for_filter)
    return text




def unwanted_character_detected(text_for_detect):
    matchs = re.search(
        r'[^a-zA-Z0-9\s{}{}{}{}{}{}{}{}{}{}]'.format(
            pattern_punctuation[1:-1],
            pattern_arabic[1:-1],
            pattern_chinese[1:-1],
            pattern_tamil[1:-1],
            pattern_thai[1:-1],
            pattern_russian[1:-1],
            pattern_korean[1:-1],
            pattern_japanese[1:-1],
            pattern_vietnamese[1:-1],
            pattern_emoji[1:-1],
        ), text_for_detect, re.I)
    if matchs:
        return True
    return False


def file_lang_filter(input_path, output_path, lang):

    output_path_dir = os.path.dirname(output_path)
    if not os.path.exists(output_path_dir):
        os.makedirs(output_path_dir)

    with open(input_path, encoding='utf8') as f_in, open(output_path, 'w', encoding='utf8') as f_out:
        for line in f_in:
            line = unwanted_character_filtered(line)
            if lang in lang_detect(line.strip()):
                f_out.write(line.strip()+'\n')


@plac.pos('input_path', "Src File/dir", type=Path)
@plac.pos('output_path', "Tgt File/dir", type=Path)
@plac.pos('lang', "language type", type=str)
def main(input_path="/home/xuancong/airflow/data/stage2", output_path="/home/xuancong/airflow/data/stage3", lang='zh'):

    os.chdir(os.path.dirname(__file__))

    if os.path.isfile(input_path):
        file_lang_filter(str(input_path), str(output_path), lang)

    elif os.path.isdir(input_path):
        for rootdir, dirs, files in os.walk(input_path):
            lang = rootdir.split('_')[-1]
            for file in files:
                if file.endswith('.sent'):
                    input_file = os.path.join(rootdir, file)
                    output_file = os.path.join(rootdir.replace(
                        str(input_path), str(output_path)), file)
                    file_lang_filter(input_file, output_file, lang)
                    print("finished {}".format(input_file), flush=True)
    else:
        print("invalid input_file")


if __name__ == "__main__":
    plac.call(main)
    print('finished all')
