import time
import os
import sys

pyp = os.path.dirname(__file__)
print('************************ wsgi start 1 ***************', file=sys.stderr)
print(pyp)
print('************************ wsgi start 2 ***************', file=sys.stderr)
sys.path.insert(0, pyp)
sys.path.insert(0, pyp + "/../ptvsd")

if 'c1' in globals():
    c1 += 1
else:
    c1 = 0
    c2 = 0

import ptvsd
if c2 == 0:
    ptvsd.enable_attach('secret', address=('localhost', 36000))
    ptvsd.wait_for_attach()
    time.sleep(5)

def application(environ,start_response):
    print('**** application start ****', file=sys.stderr)
    global c2
    c2 += 1
    status = '200 OK'
    html2 =  '</div>\n' \
           '</body>\n' \
           '</html>\n'
    html1 = '<html>\n' \
           '<body>\n' \
           '<div style="width: 100%; font-size: 40px; font-weight: bold; text-align: center;">\n' \
           'mod_wsgi Test Page\n'
    html = html1 + str(c1) + ' / ' + str(c2) + html2
    response_header = [('Content-type','text/html')]
    start_response(status,response_header)
    return [html.encode()]

