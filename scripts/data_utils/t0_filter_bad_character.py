import re
import time
import plac
from pathlib import Path
from _shared import Reg_Exp



def unwanted_character_detected(line):
    new_line = re.sub(
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
        ), '', line, 0, re.I).strip()
    return new_line



def filter_unwanted_character(line):
    new_line = re.sub(
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
        ), '', line, 0, re.I).strip()
    return new_line


@plac.opt('input_1', "Src Input File", type=Path)
@plac.opt('output_1', "Src Output File", type=Path)
def main(input_1,
         output_1):
    start_time = time.time()
    n=0
    with open(input_1, 'r', encoding='utf8') as f_in, \
            open(output_1, 'w', encoding='utf8') as f_out:
        for i, sentence in enumerate(f_in):
            f_out.write(filter_unwanted_character(sentence)+'\n')
            n+=1

    print("finished {}".format(n), flush=True)
    print("--- %s seconds ---" % (time.time() - start_time), flush=True)


if __name__ == '__main__':
    main('./data/clean_sorted.en-zh',
         './data/clean_sorted1.en-zh')
