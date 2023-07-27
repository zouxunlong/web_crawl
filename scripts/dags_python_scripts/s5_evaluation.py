import plac
import os
from pathlib import Path
from sentence_transformers import SentenceTransformer, util


model_sentence_transformers = SentenceTransformer(
    os.environ['AIRFLOW_HOME']+'/model/labse_bert_model')


def evaluate(input_file, output_file):

    output_file_dir = os.path.dirname(output_file)
    if not os.path.exists(output_file_dir):
        os.makedirs(output_file_dir)

    sentences_src_tgt = [line.strip().split(' ||| ') for line in open(input_file, encoding='utf8').readlines()]
    sentences_src = [src_tgt[0] for src_tgt in sentences_src_tgt]
    sentences_tgt = [src_tgt[1] for src_tgt in sentences_src_tgt]

    source_embedding = model_sentence_transformers.encode(
        sentences_src, show_progress_bar=False, convert_to_numpy=True, normalize_embeddings=True)

    target_embedding = model_sentence_transformers.encode(
        sentences_tgt, show_progress_bar=False, convert_to_numpy=True, normalize_embeddings=True)

    assert len(source_embedding) == len(
        target_embedding), "length of src and target don't match"

    cosine_sims = [source_embedding[i].dot(
        target_embedding[i]) for i in range(len(source_embedding))]

    with open(output_file, 'w', encoding='utf8') as f_out:
        for k in range(len(source_embedding)):
            f_out.write("{:.4f} ||| {} ||| {}\n".format(
                cosine_sims[k],
                sentences_src[k].replace("|", " "),
                sentences_tgt[k].replace("|", " ")))

@ plac.pos('input_path', "Src File/dir", type=Path)
@ plac.pos('output_path', "Tgt File/dir", type=Path)
def main(input_path="/home/zxl/airflow/data/stage4", output_path="/home/zxl/airflow/data/stage5"):

    os.chdir(os.path.dirname(__file__))

    if str(input_path).endswith('.en'):
        evaluate(str(input_path)+'2zh', str(output_path)+'2zh')
    elif str(input_path).endswith('.zh'):
        evaluate(str(input_path)+'2en', str(output_path)+'2en')


if __name__ == "__main__":
    plac.call(main)
