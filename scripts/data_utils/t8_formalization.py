import re
import string
import time
import plac
from pathlib import Path


def load_words():
    with open('/home/xuanlong/dataclean/cleaning/dataclean/words_alpha.txt') as word_file:
        valid_words = set(word_file.read().split())
    return valid_words


english_words = load_words()

punctuation = r"""!"\#$%&'()*+,-./:;<=>?@[]^_`{|}~，。、‘’“”：；【】·！￥★…《》？！（）—"""
punctuation2none_dict = str.maketrans('', '', punctuation)
pattern_punctuation = """[!"#$%&'()*+,-./:;<=>?@[\]^_`{|}~，。、‘’“”：；【】·！￥★…《》？！（）—]"""
pattern_url = r"[(http(s)?):\/\/(www\.)?a-zA-Z0-9@:%._\+~#=]{2,256}\.[a-z]{2,6}\b([-a-zA-Z0-9@:%_\+.~#?&//=]*)"
pattern_email = r"[\w\-\.]+@([\w\-]+\.)+[\w\-]{2,4}"
pattern_html = r"""<("[^"]*"|'[^']*'|[^'">])*>"""
pattern_special_charactors = r"[\x00-\x08\x0a-\x1f\x7f-\x9f\xa0]|[\ufffd\ufeff\u2000-\u200f\u2028-\u202f\u205f-\u206e]|[\↑√§¶†‡‖▪●•·]"
pattern_emoji = r'[\U0001F1E0-\U0001F1FF\U0001F300-\U0001F64F\U0001F680-\U0001FAFF\U00002702-\U000027B0]'
pattern_informal = r'[@#]|([^\w]|^)(http|https|www|com|hey|guy|kind\sof|ok|okay|oh|dude|we|you|your|my|me|haha|i)([^\w]|$)|\'(s|ve|re|t|m|ll|d|am|)\s|-\s'
pattern_I = r'I\s|([^\w]|$)us([^\w]|$)'


def formalize(line):

    line = re.sub('(^|[^\w])dlm([^\w]|$)', r'\1dalam\2', line, 0, re.I)
    line = re.sub('(^|[^\w])utk([^\w]|$)', r'\1untuk\2', line, 0, re.I)
    line = re.sub('(^|[^\w])yg([^\w]|$)', r'\1yang\2', line, 0, re.I)
    line = re.sub('(^|[^\w])krn([^\w]|$)', r'\1karena\2', line, 0, re.I)
    # line = re.sub('(^|[^\w])\[word\]2([^\w]|$)', '\1[word]-[word]\2', line, 0, re.I)
    line = re.sub('(^|[^\w])(tuan)2([^\w]|$)', r'\1\2-\2\3', line, 0, re.I)
    line = re.sub('(^|[^\w])(nyonya)2([^\w]|$)', r'\1\2-\2\3', line, 0, re.I)
    line = re.sub('(^|[^\w])(struktur)2([^\w]|$)', r'\1\2-\2\3', line, 0, re.I)

    return line


@plac.opt('input', "Src Input File", type=Path)
@plac.opt('output', "Src Output File", type=Path)
def main(input, output):
    start_time = time.time()
    with open(input, 'r', encoding='utf8') as f_in, open(output, 'w', encoding='utf8') as f_out:
        for line in f_in:
            new_line = formalize(line.strip())
            if new_line:
                f_out.write(new_line+'\n')
    print("finished ")
    print("--- %s seconds ---" % (time.time() - start_time), flush=True)


if __name__ == '__main__':
    main('/home/xuanlong/dataclean/data/train.labse.seleted.formal.en-id.id',
         '/home/xuanlong/dataclean/data/train.labse.seleted.formal2.en-id.id')
