from collections import Counter
from pathlib import Path
import plac
import time
from tqdm import tqdm
from tokenizers.pre_tokenizers import BertPreTokenizer
from tokenizers.normalizers import BertNormalizer

# nltk.download('punkt')

tokenizer = BertPreTokenizer()
normalizer = BertNormalizer()


def merge_counts(counters):
    return sum(counters, Counter())


def doc_vocab(text, k=3):
    counts = Counter(w[0] for w in tqdm(
        tokenizer.pre_tokenize_str(normalizer.normalize_str(text))))
    return Counter({w: c for w, c in counts.items() if c <= k})


def top_n_not_in_train(train_vocab, new_vocab, n):
    return (new_vocab - train_vocab).most_common(n)


@plac.opt('train_file', "Training File", type=Path)
@plac.opt('current_file', "New Data File", type=Path)
@plac.opt('n_words', "Number of new words to extract", type=int)
@plac.flg('display', "Print Top Words and Counts")
def main(train_file, current_file, n_words=100, display=False):
    """Find new vocab not present in existing training data"""
    start_time = time.time()

    print("Building Train Vocab")
    train_vocab = doc_vocab(Path(train_file).read_text())
    print("Building New Vocab")
    new_vocab = doc_vocab(Path(current_file).read_text())
    print("Calculating Diff")
    top_n = top_n_not_in_train(train_vocab, new_vocab, n_words)

    end_time = time.time()

    print(f'took {end_time-start_time} secs')

    if display:
        [print(f'{w[0]} {w[1]}') for w in top_n]

    return top_n


if __name__ == '__main__':
    import plac
    plac.call(main)
