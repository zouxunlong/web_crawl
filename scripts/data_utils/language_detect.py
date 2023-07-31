import sys
import pycld2 as cld2
import cld3
import re
from utils_data import Reg_Exp


def lang_detect(text_for_lang_detect):

    lang_detected = set()

    text_for_lang_detect = ' '.join(re.sub(r"{}|{}|{}".format(
        Reg_Exp.pattern_url,
        Reg_Exp.pattern_email,
        Reg_Exp.pattern_punctuation
    ), " ", text_for_lang_detect, 0, re.I).split()).strip().lower()

    if text_for_lang_detect:
        if re.search(Reg_Exp.pattern_arabic, text_for_lang_detect):
            lang_detected.add('ar')
        if re.search(Reg_Exp.pattern_chinese, text_for_lang_detect):
            lang_detected.add('zh')
        if re.search(Reg_Exp.pattern_tamil, text_for_lang_detect):
            lang_detected.add('ta')
        if re.search(Reg_Exp.pattern_russian, text_for_lang_detect):
            lang_detected.add('ru')
        if re.search(Reg_Exp.pattern_korean, text_for_lang_detect):
            lang_detected.add('ko')
        if re.search(Reg_Exp.pattern_japanese, text_for_lang_detect):
            lang_detected.add('ja')
        if re.search(Reg_Exp.pattern_vietnamese, text_for_lang_detect):
            lang_detected.add('vi')

        try:
            lang_by_cld2 = cld2.detect(text_for_lang_detect)[2][0][1][:2]
            lang_by_cld3 = cld3.get_language(text_for_lang_detect)[0][:2]

            if {"en"} & {lang_by_cld2, lang_by_cld3}:
                lang_detected.add('en')
            if {'ms', 'id'} & {lang_by_cld2, lang_by_cld3}:
                lang_detected.add('ms', 'id')
            if {'th'} & {lang_by_cld2, lang_by_cld3}:
                lang_detected.add('th')
            if {'vi'} & {lang_by_cld2, lang_by_cld3}:
                lang_detected.add('vi')
            if {'ta'} & {lang_by_cld2, lang_by_cld3}:
                lang_detected.add('ta')

        except Exception as err:
            exception_type, exception_object, exception_traceback = sys.exc_info()
            filename = exception_traceback.tb_frame.f_code.co_filename
            line_number = exception_traceback.tb_lineno

            print("text_for_lang_detect: ", text_for_lang_detect, flush=True)
            print("Exception type: ", exception_type, flush=True)
            print("File name: ", filename, flush=True)
            print("Line number: ", line_number, flush=True)
            print(err)

    return lang_detected
