import socket
import requests


class Back_translator:

    def __init__(self,
                 batch_size=10,
                 en2th_url='http://10.2.56.190:5001/en2th',
                 th2en_url='http://10.2.56.190:5001/th2en',
                 en2zh_port=29016,
                 zh2en_port=28015,
                 en2ms_port=28018,
                 ms2en_port=28017,
                 en2ta_port=50006,
                 ta2en_port=28019,
                 en2vi_port=28128,
                 vi2en_port=28129,
                 host='localhost'
                 ):
        self.batch_size = batch_size
        self.urls = {'en2th': en2th_url,
                     'th2en': th2en_url
                     }
        self.ports = {'en2zh': en2zh_port,
                      'zh2en': zh2en_port,
                      'en2ms': en2ms_port,
                      'ms2en': ms2en_port,
                      'en2ta': en2ta_port,
                      'ta2en': ta2en_port,
                      'en2vi': en2vi_port,
                      'vi2en': vi2en_port,
                      }
        self.host = host

    def translate_th(self, sentences_src, src, tgt):

        url = self.urls[src+'2'+tgt]

        sentences_tgt = []

        for i in range(0, len(sentences_src), self.batch_size):

            batch_sentences_src = sentences_src[i:i+self.batch_size]
            response = requests.post(
                url, json={'text': '\n'.join(batch_sentences_src)})

            sentences_tgt.extend(response.json()['translations'])

        assert len(sentences_src) == len(
            sentences_tgt), 'length of src and target do not match'

        return sentences_tgt

    def translate_zh_ms_vi_ta(self, sentences_src, src, tgt):

        with socket.socket(socket.AF_INET, socket.SOCK_STREAM) as s:
            host = self.host
            port = self.ports[src+'2'+tgt]
            s.connect((host, port))

            sentences_tgt = []

            for i in range(0, len(sentences_src), self.batch_size):

                batch_sentences_src = sentences_src[i:i+self.batch_size]
                batch_sentences_tgt = ""

                s.sendall(
                    bytes('\n'.join(batch_sentences_src)+'\n', encoding='utf8'))
                while batch_sentences_tgt.count("\n") != len(batch_sentences_src):
                    batch_sentences_tgt += s.recv(4096).decode('UTF-8')
                batch_sentences_tgt = batch_sentences_tgt.strip().split('\n')

                sentences_tgt.extend(batch_sentences_tgt)

        assert len(sentences_src) == len(
            sentences_tgt), 'length of src and target do not match'

        return sentences_tgt

    def translate(self, sentences_src, src, tgt):

        if (src, tgt) in [('en', 'th'), ('th', 'en')]:
            return self.translate_th(sentences_src, src, tgt)

        if (src, tgt) in [('en', 'zh'), ('zh', 'en'), ('en', 'ms'), ('ms', 'en'), ('en', 'ta'), ('ta', 'en'), ('en', 'vi'), ('vi', 'en')]:
            return self.translate_zh_ms_vi_ta(sentences_src, src, tgt)


if __name__ == "__main__":
    translator = Back_translator()

    # print(translator.translate(['here it is.', 'good to know.'], 'en', 'zh'))
    # print(translator.translate(['here it is.', 'good to know.'], 'en', 'ms'))
    # print(translator.translate(['here it is.', 'good to know.'], 'en', 'ta'))
    # print(translator.translate(['here it is.', 'good to know.'], 'en', 'th'))
    # print(translator.translate(['here it is.', 'good to know.'], 'en', 'vi'))
    # print(translator.translate(['இது இங்கே உள்ளது.', 'தெரிந்து கொள்வது நல்லது.'], 'ta', 'en'))
    # print(translator.translate(['Ini dia.', 'Baik untuk mengetahui.'], 'ms', 'en'))
    # print(translator.translate(['给你。', '很高兴知道。'], 'zh', 'en'))
    # print(translator.translate(['นี่ไง มันอยู่นี่แล้ว', 'ยินดีที่ได้รู้จัก'], 'th', 'en'))
    print(translator.translate(['Lượng rác đổ vào Nam Sơn trung bình khoảng 5.000 tấn/ngày (lượng rác trung bình của Hà Nội mỗi ngày vào khoảng 7-8.000 tấn).'], 'vi', 'en'))
