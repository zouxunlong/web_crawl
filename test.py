import urllib.parse
from urllib.parse import urlparse
from urllib.parse import parse_qs


url = "http://stackoverflow.com/search?q=question&date=2024-01-01"
params = {'lang':'en','tag':'python'}

url_parts = list(urllib.parse.urlparse(url))
query = dict(urllib.parse.parse_qs(url_parts[4]))
query2 = dict(urllib.parse.parse_qsl(url_parts[4]))
query.update(params)
url_parts[4] = urllib.parse.urlencode(query)

print(urllib.parse.urlunparse(url_parts))

