import time
import plac
from pathlib import Path

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
            open(input_2, 'r', encoding='utf8') as f_in_id:
        sentences_tuple_set = set()
        for (i, sentence_en), (j, sentence_id) in zip(enumerate(f_in_en), enumerate(f_in_id)):
            sentences_tuple_set.add((sentence_en.strip(), sentence_id.strip()))
    with open(output_1, 'w', encoding='utf8') as f_out_en, \
            open(output_2, 'w', encoding='utf8') as f_out_id:
        for (sentence_en, sentence_id) in sentences_tuple_set:
            f_out_en.write(sentence_en+'\n')
            f_out_id.write(sentence_id+'\n')

    print("finished {}".format(len(sentences_tuple_set)))

    sentences_tuple_set.clear()

    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    main('/home/xuanlong/dataclean/cleaning/data/V4_t1.en',
         '/home/xuanlong/dataclean/cleaning/data/V4_t1.th',
         '/home/xuanlong/dataclean/cleaning/data/V4_t2.en',
         '/home/xuanlong/dataclean/cleaning/data/V4_t2.th')
