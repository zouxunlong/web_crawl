
import html
import os
import re
import time
from simhash import Simhash
from pymongo import MongoClient


MONGO_CONNECTION_STRING = "mongodb://localhost:27017/"
mongo_client = MongoClient(MONGO_CONNECTION_STRING)

db_data_pool = mongo_client['mlops']


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


def tamil_detected(text):
    if re.search(Reg_Exp.pattern_tamil, text):
        return True
    return False


def thai_detected(text):
    if re.search(Reg_Exp.pattern_thai, text):
        return True
    return False


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
                sentences = line.strip().split('|||')

                if len(sentences) != 2:
                    continue

                beam_score = float(sentences[1][-8:])
                sentence_src = sentences[0].strip()
                sentence_tgt = sentences[1][:-8].strip()

                if unwanted_character_detected(sentence_src+' '+sentence_tgt):
                    continue

                if 'ta' in [lang_src, lang_tgt] and not tamil_detected(sentence_src+' '+sentence_tgt):
                    continue

                if 'th' in [lang_src, lang_tgt] and not thai_detected(sentence_src+' '+sentence_tgt):
                    continue

                if not sentence_src.strip() or not sentence_tgt.strip():
                    continue

                result = db_data_pool['bt_data'].update_one({'_id': Simhash(sentence_src.strip()+sentence_tgt.strip(), f=48, reg=r'[\S]').value},
                                                            {'$setOnInsert': {'sentence_src': html.unescape(sentence_src.strip()),
                                                                              'sentence_tgt': html.unescape(sentence_tgt.strip()),
                                                                              'origin_src': html.unescape(sentence_src.strip()),
                                                                              'origin_tgt': html.unescape(sentence_tgt.strip()),
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


def main(input_path='/home/xuancong/airflow/data/stage4'):
    for rootdir, dirs, files in os.walk(input_path):
        files.sort()
        for file in files:
            lang_src=file.split('.')[-1][:2]
            lang_tgt=file.split('.')[-1][-2:]
            if lang_src in ['en'] and lang_tgt in ['th']:
                source = os.path.basename(rootdir)
                input_file = os.path.join(rootdir, file)
                run_id = file.split('.')[0]
                insert_bilingual_data_from_single_files(input_file,
                                                        lang_src,
                                                        lang_tgt,
                                                        run_id,
                                                        project=[],
                                                        domain=['news'],
                                                        source=[source],
                                                        custom_tag=[],
                                                        )
                print('finished {}'.format(input_file), flush=True)
        print('finished {}'.format(rootdir), flush=True)
    print('finished', flush=True)


if __name__ == "__main__":
    import plac
    plac.call(main)

