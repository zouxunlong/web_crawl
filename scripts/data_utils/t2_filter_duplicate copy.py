import time
import plac
from pathlib import Path


@plac.opt('input_1', "Src Input File", type=Path)
@plac.opt('output_1', "Src Output File", type=Path)
def main(input_1,
         output_1):

    start_time = time.time()

    with open(input_1, 'r', encoding='utf8') as f_in:
        sentences_tuple_set = set()
        for i, line in enumerate(f_in):
            sentences = line.split('|||')
            if len(sentences) != 3:
                continue
            sentences_tuple_set.add(
                (sentences[0].strip(), sentences[1].strip(), sentences[2].strip()))

    with open(output_1, 'w', encoding='utf8') as f_out_en:
        for (score, sentence_en, sentence_id) in sentences_tuple_set:
            f_out_en.write(score+" ||| "+sentence_en+' ||| '+sentence_id+"\n")

    print("finished {}".format(len(sentences_tuple_set)))

    sentences_tuple_set.clear()

    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main('/home/xuanlong/dataclean/cleaning/data/V4.en-th',
         '/home/xuanlong/dataclean/cleaning/data/V4_2.en-th')
