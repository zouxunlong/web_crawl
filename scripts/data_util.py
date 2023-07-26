
import html
import os
import re
import time
from simhash import Simhash
from pymongo import MongoClient
from datetime import datetime


MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
mongo_client = MongoClient(MONGO_CONNECTION_STRING)

db_data_pool = mongo_client['mlops']
db_worksheet = mongo_client['mlops_worksheet']


class Reg_Exp:
    pattern_punctuation = r"""[!?,.:;"#$£€%&'()+-/<≤=≠≥>@[\]^_{|}，。、—‘’“”：；【】￥…《》？！（）]"""
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


def unwanted_character_detected(text_for_detect):
    matchs = re.search(
        r'[^a-zA-Z0-9\s\t{}{}{}{}{}{}{}{}{}{}]'.format(
            Reg_Exp.pattern_punctuation[1:-1],
            Reg_Exp.pattern_arabic[1:-1],
            Reg_Exp.pattern_chinese[1:-1],
            Reg_Exp.pattern_tamil[1:-1],
            Reg_Exp.pattern_thai[1:-1],
            Reg_Exp.pattern_russian[1:-1],
            Reg_Exp.pattern_korean[1:-1],
            Reg_Exp.pattern_japanese[1:-1],
            Reg_Exp.pattern_vietnamese[1:-1],
            Reg_Exp.pattern_emoji[1:-1],
        ), text_for_detect, re.I)
    if matchs:
        return True
    return False


def insert_bilingual_data_from_seperate_files(file_path_src,
                                              file_path_tgt,
                                              lang_src,
                                              lang_tgt,
                                              project=[],
                                              domain=[],
                                              source=[],
                                              custom_tag=[]):
    try:
        with open(file_path_src, encoding='utf8') as file_src,\
                open(file_path_tgt, encoding='utf8') as file_tgt:
            n = 0
            k = 0
            for (i, line_src), (j, line_tgt) in zip(enumerate(file_src), enumerate(file_tgt)):

                if unwanted_character_detected(line_src + line_tgt):
                    continue

                if not line_src.strip() or not line_tgt.strip():
                    continue

                result = db_data_pool[lang_src+'||'+lang_tgt].update_one({'_id': Simhash(line_src.strip()+line_tgt.strip(), f=48, reg=r'[\S]').value},
                                                                         {'$setOnInsert': {'sentence_src': html.unescape(line_src.strip()),
                                                                                           'sentence_tgt': html.unescape(line_tgt.strip()),
                                                                                           'lang_src': lang_src,
                                                                                           'lang_tgt': lang_tgt,
                                                                                           'language_type': lang_src+'||'+lang_tgt,
                                                                                           'project': project,
                                                                                           'domain': domain,
                                                                                           'source': source,
                                                                                           'custom_tag': custom_tag}},
                                                                         upsert=True)
                if result.modified_count != 0:
                    n += result.modified_count
                if result.upserted_id != None:
                    k += 1

            print("modified_count: {} documents.".format(n), flush=True)
            print("upserted_count: {} documents.".format(k), flush=True)

    except Exception as err:
        print(err, flush=True)
        print(i, flush=True)


def insert_bilingual_data_from_single_files(file_path,
                                            lang_src,
                                            lang_tgt,
                                            run_id,
                                            project=[],
                                            domain=[],
                                            source=[],
                                            custom_tag=[],
                                            ):
    try:
        with open(file_path, encoding='utf8') as file_in:
            n = 0
            k = 0
            for line in file_in:
                sentences = line.split('|||')

                if len(sentences) != 2:
                    continue

                beam_score = float(sentences[1][-8:])
                sentence_src = sentences[0].strip()
                sentence_tgt = sentences[1][:-8].strip()

                if unwanted_character_detected(sentence_src+' '+sentence_tgt):
                    continue

                if not sentence_src or not sentence_tgt:
                    continue

                result = db_data_pool['bt_data'].update_one({'_id': Simhash(sentence_src.strip()+sentence_tgt.strip(), f=48, reg=r'[\S]').value},
                                                            {'$setOnInsert': {'sentence_src': html.unescape(sentence_src.strip()),
                                                                              'sentence_tgt': html.unescape(sentence_tgt.strip()),
                                                                              'lang_src': lang_src,
                                                                              'lang_tgt': lang_tgt,
                                                                              'language_type': lang_src+'2'+lang_tgt,
                                                                              'project': project,
                                                                              'domain': domain,
                                                                              'source': source,
                                                                              'beam_score': beam_score,
                                                                              'custom_tag': custom_tag,
                                                                              'run_id': run_id}},
                                                            upsert=True)
                if result.modified_count != 0:
                    n += result.modified_count
                if result.upserted_id != None:
                    k += 1

            print("modified_count: {} documents.".format(n), flush=True)
            print("upserted_count: {} documents.".format(k), flush=True)

    except Exception as err:
        print(err, flush=True)


def detokenize_zh(text):
    text = re.sub(r'\s?([\u4e00-\u9fff，。、—‘’“”：；【】￥…《》？！（）])\s?', r'\1', text)
    return text


def detokenize_en(text):
    if not text.strip():
        return
    step1 = text.replace("`` ", '"').replace(
        " ''", '"').replace('. . .', '...')
    step2 = step1.replace(" ( ", " (").replace(" ) ", ") ")
    step3 = re.sub(r' ([.,:;?!%]+)([ \'"`])', r"\1\2", step2)
    step4 = re.sub(r' ([.,:;?!%]+)$', r"\1", step3)
    step5 = step4.replace(" '", "'").replace(" n't", "n't").replace(
        "can not", "cannot")
    step6 = step5.replace(" ` ", " '")
    return step6.strip()


def tokenize_by_char_zh(sent):
    chars = re.split(r'([\u4e00-\u9fff\W])', sent)
    chars = [w for w in chars if len(w.strip()) > 0]
    return chars


def recaser_vi_en_ms(sent):

    if not sent or not sent.strip():
        return
    words_list = sent.split()
    for i, word in enumerate(words_list):
        if i == 0 or words_list[i-1] in '.?!"':
            words_list[i] = word[0].upper()+word[1:]
    return ' '.join(words_list)


def string_to_date():

    results = db_data_pool['bt_data'].find({"$and": [
        {'run_id': { "$exists": True }},
    ]}).sort("_id",1)
    for result in results:
        if "collect_date" not in result.keys():
            date_string = result['run_id'][11:21]
            r = db_data_pool['bt_data'].update_one({'_id': result["_id"]},
                                                {'$set': {'collect_date': datetime.strptime(date_string, '%Y-%m-%d'),
                                                            }})
    print("finished", flush=True)

def main(input_path):
    for rootdir, dirs, files in os.walk(input_path):
        for file in files:
            if file.endswith('.vi2en'):
                source = os.path.basename(rootdir)
                input_file = os.path.join(rootdir, file)
                run_id = file.split('.')[0]
                insert_bilingual_data_from_single_files(input_file,
                                                        'zh',
                                                        'en',
                                                        run_id,
                                                        project=[],
                                                        domain=['news'],
                                                        source=[source],
                                                        custom_tag=[],
                                                        )
    print('finished')


if __name__ == "__main__":

    string_to_date()
