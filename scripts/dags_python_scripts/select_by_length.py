
def selected(sent):
    if len(sent.split()) in range(30,50):
        return True
    return False

def filter_by_length(file):
    with open(file, encoding='utf8') as f_in, open(file+'.seleted','w',encoding='utf8') as f_out:
        for line in f_in:
            if selected(line):
                f_out.write(line)

def main():
    file_path='/home/zxl/airflow/data/stage3_en.sent'
    filter_by_length(file_path)

if __name__=="__main__":
    main()