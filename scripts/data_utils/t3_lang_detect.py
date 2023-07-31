from pathlib import Path
import time
import fasttext
import plac


model_fasttext = fasttext.load_model('../model/lid.176.bin')


def lang_matches(lines, langs, k):
    langs = [lang.strip() for lang in langs.split(',')]
    allowed_labels = {f'__label__{lang}' for lang in langs}

    def check_lang_match(prediction):
        return len(allowed_labels.intersection(set(prediction))) > 0

    preds = model_fasttext.predict([l.strip() for l in lines], k=k)[0]
    matches = [check_lang_match(p) for p in preds]
    return matches


def filter_pairs(src_lines,
                 tgt_lines,
                 src_lang,
                 tgt_lang,
                 k=2,
                 filter_only_src=False):

    src_matches = lang_matches(src_lines, src_lang, k)
    if filter_only_src:
        # accept all targets
        tgt_matches = [True] * len(tgt_lines)
    tgt_matches = lang_matches(tgt_lines, tgt_lang, k)

    filter_critera = [
        s_match and t_match
        for s_match, t_match in zip(src_matches, tgt_matches)
    ]

    filtered_pairs = [
        (s, t) for s, t, match in zip(src_lines, tgt_lines, filter_critera)
        if match
    ]
    filtered_src, filtered_tgt = list(zip(*filtered_pairs))

    assert len(filtered_src) == len(filtered_tgt)

    return filtered_src, filtered_tgt


@plac.opt('a_input', "Src Input File", type=Path)
@plac.opt('b_input', "Tgt Input File", type=Path)
@plac.opt('c_output', "Src Output File", type=Path)
@plac.opt('d_output', "Tgt Output File", type=Path)
@plac.opt('src_lang', "Comma seperated allowed src langs", type=str)
@plac.opt('tgt_lang', "Comma seperated allowed tgt langs", type=str)
@plac.opt('k', "Detected language should be top-k for a match", type=int)
@plac.flg('filter_only_src', "Filter Only According to Source Side")
def main(a_input,
         b_input,
         c_output,
         d_output,
         src_lang,
         tgt_lang,
         k=2,
         filter_only_src=False):

    start_time = time.time()

    src_lines = open(a_input).readlines()
    tgt_lines = open(b_input).readlines()

    assert len(src_lines) == len(
        tgt_lines), "length of src and target don't match"

    print(f'Input of {len(src_lines)} lines')

    filtered_src, filtered_tgt = filter_pairs(src_lines, tgt_lines, src_lang,
                                              tgt_lang, k, filter_only_src)

    assert len(filtered_src) == len(filtered_tgt)

    print(f'Filtered {len(filtered_src)} lines')

    open(c_output, 'w').writelines(filtered_src)
    open(d_output, 'w').writelines(filtered_tgt)

    print("finished ")
    print("--- %s seconds ---" % (time.time() - start_time))


if __name__ == '__main__':
    # plac.call(main)
    main('/home/xuanlong/dataclean/data.t2.en',
         '/home/xuanlong/dataclean/data.t2.id',
         '/home/xuanlong/dataclean/data.t3.en',
         '/home/xuanlong/dataclean/data.t3.id',
         'en',
         'id',
         1,
         True)
