from sentence_transformers import SentenceTransformer, util

# import torch
# torch.cuda.set_device(1)


model_sentence_transformers = SentenceTransformer('../model/labse_bert_model')

def embedding_saving(sentences_src, sentences_tgt, file_path_out):

    source_embedding = model_sentence_transformers.encode(
        sentences_src, show_progress_bar=False, convert_to_numpy=True, normalize_embeddings=True)

    target_embedding = model_sentence_transformers.encode(
        sentences_tgt, show_progress_bar=False, convert_to_numpy=True, normalize_embeddings=True)

    assert len(source_embedding) == len(
        target_embedding), "length of src and target don't match"

    cosine_scores = util.cos_sim(source_embedding, target_embedding)

    with open(file_path_out, 'a', encoding='utf8') as f_out:
        for k in range(len(cosine_scores)):
            cosine_score = cosine_scores[k][k]
            if cosine_score >= 0.7:
                f_out.write("{:.4f} | {} ||| {}\n".format(
                    cosine_score, sentences_src[k].replace("|", " "), sentences_tgt[k].replace("|", " ")))


def clean_with_score(file_path_src, file_path_tgt, file_path_out):
    with open(file_path_src, encoding='utf8') as file_src, \
            open(file_path_tgt, encoding='utf8') as file_tgt:

        sentences_src = []
        sentences_tgt = []

        for (i, sentence_src), (j, sentence_tgt) in zip(enumerate(file_src), enumerate(file_tgt)):
            if len(sentence_src.strip()) > 15 and len(sentence_tgt.strip()) > 15:
                sentences_src.append(sentence_src.strip())
                sentences_tgt.append(sentence_tgt.strip())

            if (i+1) % 50000 == 0:
                embedding_saving(sentences_src, sentences_tgt,
                                 file_path_out)
                sentences_src.clear()
                sentences_tgt.clear()
                print(i, flush=True)

        embedding_saving(sentences_src, sentences_tgt, file_path_out)

    print("finished " + file_path_out, flush=True)


def main():

    clean_with_score('/home/xuanlong/dataclean/data/Total/train.filtered.en',
                     '/home/xuanlong/dataclean/data/Total/train.filtered.id',
                     '/home/xuanlong/dataclean/data/Total/train.filtered.labse.en-id')



if __name__ == '__main__':
    main()

