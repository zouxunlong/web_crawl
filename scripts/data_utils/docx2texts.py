import re
from docx import Document
from docx import Document
from docx.oxml.shared import qn
from docx.text.paragraph import Paragraph
from docx.text.run import Run, _Text
from utils_data import Reg_Exp

def get_paragraph_runs(paragraph):
    def _get(node):
        for child in node:
            if not (child.tag == qn('w:drawing') or child.tag == qn('w:pict')):
                if child.tag == qn('w:r'):
                    yield Run(child, node)
                yield from _get(child)
    return list(_get(paragraph._element))


def get_paragraph_text(paragraph):
    text = ''
    for run in paragraph.runs:
        text += run.text
    return text


def set_paragraph_text(paragraph, text):
    runs = paragraph.runs
    for run in runs:
        if run.text.strip():
            run._r.getparent().remove(run._r)
    for child in paragraph._element:
        if child.tag == qn('w:hyperlink'):
            if len(child) == 0:
                child.getparent().remove(child)
    paragraph.add_run(text)


Paragraph.runs = property(fget=lambda self: get_paragraph_runs(self))
Paragraph.text = property(fget=lambda self: get_paragraph_text(self),
                          fset=lambda self, text: set_paragraph_text(self, text))


def get_all_texts(node):
    def _get(node):
        for child in node:
            if child.tag == qn('w:t'):
                yield _Text(child)._t
            yield from _get(child)
    return list(_get(node._element))


def get_all_runs(node):
    def _get(node):
        for child in node:
            if child.tag == qn('w:r'):
                yield Run(child, node)
            yield from _get(child)
    return list(_get(node._element))


def get_all_paragraphs(node):
    def _get(node):
        for child in node:
            if child.tag == qn('w:p'):
                yield Paragraph(child, node)
            yield from _get(child)
    return list(_get(node._element))


def texts_extract(docx_path):

    if not str(docx_path).endswith('.docx'):
        return []

    wordDoc = Document(docx_path)
    items = get_all_paragraphs(wordDoc)
    for section in wordDoc.sections:
        header = section.header
        footer = section.footer
        items += get_all_paragraphs(header)
        items += get_all_paragraphs(footer)

    texts = [
        re.sub(
            r'[^a-zA-Z0-9\s\t{}{}{}{}{}{}{}{}{}{}]'.format(
                Reg_Exp.pattern_punctuation[1:-1],
                Reg_Exp.pattern_arabic[1:-1],
                Reg_Exp.pattern_chinese[1:-1],
                Reg_Exp.pattern_tamil[1:-1],
                Reg_Exp.pattern_thai[1:-1],
                Reg_Exp.pattern_russian[1:-1],
                Reg_Exp.pattern_korean[1:-1],
                Reg_Exp.pattern_japanese[1:-1],
                Reg_Exp.pattern_vietnamese[1:-1],
                Reg_Exp.pattern_emoji[1:-1],
            ), ' ', item.text.strip()).strip()
        for item in items if item.text.strip()]

    texts = [
        re.sub(
            r'^([0-9i]{1,3}\.|[•-])([^0-9])',
            '\\2',
            ' '.join(text.split())
        ).replace('|||', ' ').strip()
        for text in texts if text.strip()]

    additional_texts = set()

    for text in texts:
        if re.search(r'[\(\[（【】）\]\)]', text):
            # find all Brackets content and the words sequence in front of Brackets,
            # return format: ('words sequence','word infront','Brackets content')
            results = re.findall(
                r'(?=((\s[^\s】）\]\)]+){2,8})\s?[\(\[（【](.+?)[】）\]\)])', text)
            results2 = re.findall(
                r'(?=(^[^\(\[（【】）\]\)]+)\s?[\(\[（【](.+?)[】）\]\)])', text)
            for tuple in results:
                additional_texts.update(tuple)
            for tuple in results2:
                additional_texts.update(tuple)

    texts.extend([text.strip() for text in additional_texts if text.strip()])

    return texts


