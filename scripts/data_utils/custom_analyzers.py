from whoosh.analysis.acore import Token

from tltk import nlp
class ThAnalyzer():

    def __call__(self, value, positions=False, chars=False,
                 keeporiginal=False, removestops=False,
                 start_pos=0, start_char=0, mode='', **kwargs):
        results = nlp.word_segment(value).replace('<s/>', '').split("|")
        results = [x for x in results if x] #As I replaced <s/> with empty string, empty string became an item in results.
        for term in results:
            t = Token(positions, chars, removestops=removestops, mode=mode,
                      **kwargs)
            t.text = term
            t.boost = 1.0
            if keeporiginal:
                t.original = term
            if positions:
                t.pos = start_pos + 1
            if chars:
                t.startchar = start_char
                t.endchar = start_char + len(term)
            yield t


from underthesea import word_tokenize
class ViAnalyzer():

    def __call__(self, value, positions=False, chars=False,
                 keeporiginal=False, removestops=False,
                 start_pos=0, start_char=0, mode='', **kwargs):
        result = word_tokenize(value)
        for term in result:
            term = term.lower()
            t = Token(positions, chars, removestops=removestops, mode=mode,
                      **kwargs)
            t.text = term
            t.boost = 1.0
            if keeporiginal:
                t.original = term
            if positions:
                t.pos = start_pos + 1
            if chars:
                t.startchar = start_char
                t.endchar = start_char + len(term)
            yield t


from indicnlp.normalize import indic_normalize
from indicnlp.tokenize import indic_tokenize
class TaAnalyzer():

    def __call__(self, value, positions=False, chars=False,
                 keeporiginal=False, removestops=False,
                 start_pos=0, start_char=0, mode='', **kwargs):
        normalizer = indic_normalize.TamilNormalizer()
        normalized_text = normalizer.normalize(value)
        results = indic_tokenize.trivial_tokenize_indic(normalized_text)
        for term in results:
            t = Token(positions, chars, removestops=removestops, mode=mode,
                      **kwargs)
            t.text = term
            t.boost = 1.0
            if keeporiginal:
                t.original = term
            if positions:
                t.pos = start_pos + 1
            if chars:
                t.startchar = start_char
                t.endchar = start_char + len(term)
            yield t
