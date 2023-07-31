import os
import re
import sys
from sentence_transformers import SentenceTransformer, util
import pycld2 as cld2
import cld3
import fasttext


model_fasttext = fasttext.load_model('../model/lid.176.bin')
model_sentence_transformers = SentenceTransformer('../model/labse_bert_model')


punctuation = r"""!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~，。、‘’“”：；【】·！￥★…《》？！（）—"""
pattern_punctuation = r"""[!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~，。、‘’“”：；【】·！￥★…《》？！（）—]"""
pattern_url = r"[(http(s)?):\/\/(www\.)?a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)"
pattern_email = r"[\w\-\.]+@([\w\-]+\.)+[\w\-]{2,4}"
pattern_arabic = r"[\u0600-\u06FF]"
pattern_chinese = r"[\u4e00-\u9fff]"
pattern_tamil = r"[\u0B80-\u0BFF]"
pattern_russian = r"[\u0400-\u04FF]"
pattern_korean = r"[\uac00-\ud7a3]"
pattern_japanese = r"[\u3040-\u30ff\u31f0-\u31ff]"
pattern_vietnamese = r"[àáãạảăắằẳẵặâấầẩẫậèéẹẻẽêềếểễệđìíĩỉịòóõọỏôốồổỗộơớờởỡợùúũụủưứừửữựỳỵỷỹýÀÁÃẠẢĂẮẰẲẴẶÂẤẦẨẪẬÈÉẸẺẼÊỀẾỂỄỆĐÌÍĨỈỊÒÓÕỌỎÔỐỒỔỖỘƠỚỜỞỠỢÙÚŨỤỦƯỨỪỬỮỰỲỴỶỸÝ]"


def get_dp(M):
    m = len(M)
    n = len(M[0])
    dp = [[0]*n for i in range(m)]
    dp[0] = [sum(M[0][:i+1]) for i in range(n)]
    for i in range(1, m):
        dp[i][0] = dp[i-1][0] + M[i][0]

    for i in range(1, m):
        for j in range(1, n):
            dp[i][j] = max(dp[i-1][j], dp[i][j-1]) + M[i][j]
    return dp


def retrieve_coordinate(dp, coordinate):
    if coordinate[0] == 0:
        return (coordinate[0], coordinate[1]-1)
    elif coordinate[1] == 0:
        return (coordinate[0]-1, coordinate[1])
    elif dp[coordinate[0]-1][coordinate[1]] >= dp[coordinate[0]][coordinate[1]-1]:
        return (coordinate[0]-1, coordinate[1])
    else:
        return (coordinate[0], coordinate[1]-1)


def get_path(M):

    coordinate = (len(M)-1, len(M[0])-1)
    path = [coordinate]
    dp = get_dp(M)

    while coordinate != (0, 0):
        coordinate = retrieve_coordinate(dp, coordinate)
        path.append(coordinate)
    path.reverse()
    return path


def lang_detect(text_for_lang_detect):

    lang_detected = set()

    text_for_lang_detect = ' '.join(re.sub("{}|{}|{}".format(
        pattern_url, pattern_email, pattern_punctuation), " ", text_for_lang_detect, 0, re.I).split()).strip().lower()

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

        try:
            lang_by_cld2 = cld2.detect(text_for_lang_detect)[2][0][1]
            lang_by_cld3 = cld3.get_language(text_for_lang_detect)[0]
            lang_by_fasttext = model_fasttext.predict(
                text_for_lang_detect)[0][0][-2:]

            if {"en"} & {lang_by_cld2, lang_by_cld3, lang_by_fasttext}:
                lang_detected.add('en')
            if {'ms'} & {lang_by_cld2, lang_by_cld3, lang_by_fasttext}:
                lang_detected.add('ms')
            if {'id'} & {lang_by_cld2, lang_by_cld3, lang_by_fasttext}:
                lang_detected.add('id')
            if {'vi'} & {lang_by_cld2, lang_by_cld3, lang_by_fasttext}:
                lang_detected.add('vi')

        except Exception as err:
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = exception_traceback.tb_frame.f_code.co_filename
            line_number = exception_traceback.tb_lineno

            print("Exception type: ", exception_type, flush=True)
            print("File name: ", filename, flush=True)
            print("Line number: ", line_number, flush=True)
            print(err)

    return lang_detected


def embedding_saving(sentences_src, sentences_tgt, file_path_out):
    source_embedding = model_sentence_transformers.encode(
        sentences_src, convert_to_numpy=True, normalize_embeddings=True)

    target_embedding = model_sentence_transformers.encode(
        sentences_tgt, convert_to_numpy=True, normalize_embeddings=True)

    cosine_scores = util.cos_sim(source_embedding, target_embedding)

    path = get_path(cosine_scores)

    with open(file_path_out, 'a', encoding='utf-8') as fOUT:
        for k in range(len(path)):
            cosine = cosine_scores[path[k][0]][path[k][1]]
            if cosine >= 0.7:
                fOUT.write("{:.4f} | {} | {}\n".format(
                    cosine, sentences_src[path[k][0]].replace("|", " "), sentences_tgt[path[k][1]].replace("|", " ")))


def clean_with_score(file_path_src, file_path_tgt, file_path_out, src_lang, tgt_lang):
    with open(file_path_src, encoding='utf-8') as file_src, \
            open(file_path_tgt, encoding='utf-8') as file_tgt:

        sentences_src = []
        sentences_tgt = []

        for (i, sentence_src), (j, sentence_tgt) in zip(enumerate(file_src), enumerate(file_tgt)):
            if len(sentence_src.strip()) > 20 and len(sentence_tgt.strip()) > 2:

                if (lang_detect(sentence_src.strip()) == {src_lang}) and (
                    (tgt_lang in {'zh', 'ta', 'vi'} and lang_detect(sentence_tgt.strip()) == {tgt_lang}) or (
                        tgt_lang in {'ms', 'id'} and not lang_detect(sentence_tgt.strip())-{'ms', 'id'} )):
                    sentences_src.append(sentence_src.strip())
                    sentences_tgt.append(sentence_tgt.strip())

            if (i+1) % 500 == 0:
                embedding_saving(sentences_src, sentences_tgt, file_path_out)
                sentences_src.clear()
                sentences_tgt.clear()

        embedding_saving(sentences_src, sentences_tgt, file_path_out)

    print("finished " + file_path_out)


if __name__ == '__main__':

    rootdir = '/home/xuanlong/dataclean/data'

    for root, dirs, files in os.walk(rootdir):
        for file in files:
            if root.split(r'/')[-1] not in ['ccaligned', 'ccmatrix', 'wikimatrix', 'wikimedia', 'wikimedia_v1', 'wikimedia_v20210402']:

                if os.path.splitext(file)[1] in {'.ta', '.zh', '.vi', '.ms', '.id'}:
                    file_path_src = os.path.join(
                        root, os.path.splitext(file)[0]+'.en')
                    file_path_tgt = os.path.join(root, file)
                    file_path_out = os.path.join(
                        root, os.path.splitext(file)[0])
                    src_lang = 'en'
                    tgt_lang = file.split('.')[-1]
                    clean_with_score(file_path_src, file_path_tgt,
                                     file_path_out, src_lang, tgt_lang)
