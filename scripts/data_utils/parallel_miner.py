from sentence_transformers import SentenceTransformer
import numpy as np
import faiss


class Prallel_miner:

    def __init__(self, knn_neighbors=6, min_matching_score=0.9999, min_cos_sim=0.6, model_path_or_name='LaBSE', sort_by_cos=False):
        self.knn_neighbors = knn_neighbors
        self.min_matching_score = min_matching_score
        self.min_cos_sim = min_cos_sim
        self.model = SentenceTransformer(model_path_or_name)
        self.sort_by_cos = sort_by_cos

    def score_candidates(self, x, y, candidate_inds, fwd_mean, bwd_mean):
        scores = np.zeros(candidate_inds.shape)
        for i in range(scores.shape[0]):
            for j in range(scores.shape[1]):
                k = candidate_inds[i, j]
                scores[i, j] = (x[i].dot(y[k])) / ((fwd_mean[i]+bwd_mean[k])/2)
        return scores

    def kNN(self, x, y, k):

        idx = faiss.IndexFlatIP(y.shape[1])
        idx.add(y)
        sim, ind = idx.search(x, k)

        return sim, ind

    def list_to_set(self, text_list_dict):
        text_set_dict = {}
        for lang, text_list in text_list_dict.items():
            if lang in ['en', 'ms', 'ta', 'id']:
                _2_gram_list = [' '.join(text_list[i:i+2])
                                for i in range(len(text_list)-1)]
                _3_gram_list = [' '.join(text_list[i:i+3])
                                for i in range(len(text_list)-1)]

            if lang in ['zh']:
                _2_gram_list = [''.join(text_list[i:i+2])
                                for i in range(len(text_list)-1)]
                _3_gram_list = [''.join(text_list[i:i+3])
                                for i in range(len(text_list)-1)]

            text_set = set(text_list)
            text_set_n_gram = set(
                [*_2_gram_list]) - text_set

            text_set_dict[lang] = (text_set, set())
        return text_set_dict

    def sentence_matching(self, tuple_src, tuple_tgt):

        sentences_src_extended = [*tuple_src[0], *tuple_src[1]]
        sentences_tgt_extended = [*tuple_tgt[0], *tuple_tgt[1]]
        sentence_pair = []
        sentence_pair_num = 0

        if sentences_src_extended and sentences_tgt_extended:
            x = self.model.encode(
                sentences_src_extended, show_progress_bar=False, convert_to_numpy=True, normalize_embeddings=True)

            y = self.model.encode(
                sentences_tgt_extended, show_progress_bar=False, convert_to_numpy=True, normalize_embeddings=True)

            x2y_sim, x2y_ind = self.kNN(x, y, k=min(
                [len(x), len(y), self.knn_neighbors]))
            x2y_mean = x2y_sim.mean(axis=1)

            y2x_sim, y2x_ind = self.kNN(y, x, k=min(
                [len(x), len(y), self.knn_neighbors]))
            y2x_mean = y2x_sim.mean(axis=1)

            fwd_scores = self.score_candidates(
                x, y, x2y_ind, x2y_mean, y2x_mean)
            bwd_scores = self.score_candidates(
                y, x, y2x_ind, y2x_mean, x2y_mean)

            fwd_best = x2y_ind[np.arange(
                x.shape[0]), fwd_scores.argmax(axis=1)]
            bwd_best = y2x_ind[np.arange(
                y.shape[0]), bwd_scores.argmax(axis=1)]

            indices = np.stack([np.concatenate([np.arange(x.shape[0]), bwd_best]),
                                np.concatenate([fwd_best, np.arange(y.shape[0])])], axis=1)

            scores = np.concatenate(
                [fwd_scores.max(axis=1), bwd_scores.max(axis=1)])

            scores_cos_sim = np.array(
                [x[item[0]].dot(y[item[1]]) for item in indices])

            seen_src, seen_trg = set(), set()

            if self.sort_by_cos:
                for i in np.argsort(-scores_cos_sim):
                    src_ind, trg_ind = indices[i]
                    src_ind = int(src_ind)
                    trg_ind = int(trg_ind)

                    if scores_cos_sim[i] <= self.min_cos_sim:
                        break

                    if src_ind not in seen_src and trg_ind not in seen_trg:
                        if src_ind in range(len(tuple_src[0])) or trg_ind in range(len(tuple_tgt[0])):
                            if scores[i] >= self.min_matching_score:
                                seen_src.add(src_ind)
                                seen_trg.add(trg_ind)
                                sentence_pair.append((sentences_src_extended[src_ind].replace(
                                    "|", " "), sentences_tgt_extended[trg_ind].replace("|", " ")))
                                sentence_pair_num += 1
            else:
                for i in np.argsort(-scores):
                    src_ind, trg_ind = indices[i]
                    src_ind = int(src_ind)
                    trg_ind = int(trg_ind)

                    if scores[i] <= self.min_matching_score:
                        break

                    if src_ind not in seen_src and trg_ind not in seen_trg:
                        if src_ind in range(len(tuple_src[0])) or trg_ind in range(len(tuple_tgt[0])):
                            if scores_cos_sim[i] >= self.min_cos_sim:
                                seen_src.add(src_ind)
                                seen_trg.add(trg_ind)
                                sentence_pair.append((sentences_src_extended[src_ind].replace(
                                    "|", " "), sentences_tgt_extended[trg_ind].replace("|", " ")))
                                sentence_pair_num += 1
        return sentence_pair
