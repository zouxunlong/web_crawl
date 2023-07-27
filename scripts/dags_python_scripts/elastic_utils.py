
import re


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

