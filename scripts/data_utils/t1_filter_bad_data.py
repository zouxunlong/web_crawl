import re
import string
import time
import plac
from pathlib import Path
from _shared import Reg_Exp


def load_english_words():
    with open('/home/xuanlong/service_extraction_5002/utils_dataclean/words_alpha.txt') as word_file:
        valid_words = set(word_file.read().split())
    return valid_words


english_words = load_english_words()


def mis_spelling_detected(en_sent):

    non_en_words = [word for word in en_sent.split()
                    if
                    (not re.search('[^a-z]', word))
                    and
                    (word not in english_words)]
    if non_en_words:
        return True
    return False


def emoji_detected(text_for_detect):
    if re.search(Reg_Exp.pattern_emoji, text_for_detect):
        return True
    return False


def informal_detected(text_for_detect):
    if re.search(Reg_Exp.pattern_informal, text_for_detect, re.I):
        return True
    if re.search(Reg_Exp.pattern_I, text_for_detect):
        return True
    return False


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


def long_detected(text_for_detect):

    if len(text_for_detect.split()) > 10:
        return True
    return False


def short_detected(text_for_detect):
    sub_strings = re.split(Reg_Exp.pattern_punctuation, text_for_detect)
    if max([len(substring.split()) for substring in sub_strings]) < 7:
        return True
    return False


def chaotic_detected(text_for_chaotic_detect):
    alphabetic_text = re.sub('[^a-zA-Z]', '', text_for_chaotic_detect)
    if len(alphabetic_text)/len(text_for_chaotic_detect) < 0.5:
        return True
    return False


def is_filtered(sentence_src, sentence_tgt):

    # if long_detected(sentence_src):
    #     return True

    # if short_detected(sentence_src):
    #     return True

    # if informal_detected(sentence_src):
    #     return True

    if mis_spelling_detected(sentence_src):
        return True

    if unwanted_character_detected(sentence_src+' '+sentence_tgt):
        return True

    # if emoji_detected(sentence_src+' '+sentence_tgt):
    #     return True

    # if re.search("{}|{}|{}".format(pattern_special_charactors, pattern_html, pattern_email), sentence_src+' '+sentence_tgt, re.I):
    #     return True

    if len(re.findall(':', sentence_src, re.I)) != len(re.findall(':', sentence_tgt, re.I)):
        return True

    if len(re.findall('\/', sentence_src, re.I)) != len(re.findall('\/', sentence_tgt, re.I)):
        return True

    if len(re.findall(r'\\', sentence_src, re.I)) != len(re.findall(r'\\', sentence_tgt, re.I)):
        return True

    if len(re.findall('@', sentence_src, re.I)) != len(re.findall('@', sentence_tgt, re.I)):
        return True

    if len(re.findall('#', sentence_src, re.I)) != len(re.findall('#', sentence_tgt, re.I)):
        return True

    # if chaotic_detected(sentence_src):
    #     return True

    return False


@plac.opt('input_1', "Src Input File", type=Path)
@plac.opt('input_2', "Tgt Input File", type=Path)
@plac.opt('output_1', "Src Output File", type=Path)
@plac.opt('output_2', "Tgt Output File", type=Path)
def main(input_1,
         input_2,
         output_1,
         output_2):
    start_time = time.time()
    with open(input_1, 'r', encoding='utf8') as f_in_en, \
            open(input_2, 'r', encoding='utf8') as f_in_id, \
            open(output_1, 'w', encoding='utf8') as f_out_en, \
            open(output_2, 'w', encoding='utf8') as f_out_id:
        for (i, sentence_en), (j, sentence_id) in zip(enumerate(f_in_en), enumerate(f_in_id)):
            if is_filtered(sentence_en.strip(), sentence_id.strip()):
                continue
            f_out_en.write(sentence_en)
            f_out_id.write(sentence_id)

    print("finished {}".format(i), flush=True)
    print("--- %s seconds ---" % (time.time() - start_time), flush=True)


if __name__ == '__main__':
    main('/home/xuanlong/dataclean/cleaning/data/V4.en',
         '/home/xuanlong/dataclean/cleaning/data/V4.th',
         '/home/xuanlong/dataclean/cleaning/data/V4_t1.en',
         '/home/xuanlong/dataclean/cleaning/data/V4_t1.th',)
