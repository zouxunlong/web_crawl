from pathlib import Path
import plac


def filter_by_query(file, query):
    with open(file, encoding='utf8') as f_in, open(file+'.seleted','w',encoding='utf8') as f_out:
        for line in f_in:
            if query.lower() in line.lower():
                f_out.write(line)


@plac.opt('file', "queried file", type=Path)
@plac.opt('query', "query term", type=str)
def main(file, query):
    filter_by_query(str(file), query)

if __name__ == '__main__':
    plac.call(main)