#! /usr/bin/env python3

'''get the current LIEN amount from txlife api for a given polnum'''
import re
import requests
# import time

def get_lien_amount(polnum:str) -> float:
    url = f'https://txlife_url/{polnum}'
    url = 'http://google.com'
    response = requests.get(url)
    # contents = response.readlines()
    contents = requests.get(url,stream=True).iter_lines()
    print(list(contents)[0])

    # get lines that contains 'LIEN'
    liens = []

    with open('temp/txlife.txt', 'r', encoding='utf-8') as f:
        contents = f.readlines()

    for i, line in enumerate(contents):
        for _ in re.finditer(re.compile('LIEN'), line):
            liens.append(line)

    if len(liens) == 0:
        # no lien record was found
        return 0

    # get the most up-to-date cumulated lien amount
    last_lien = liens[-1]

    # Each record of lien amount looks like this:
    # LTCC: $1,234.50(M), $2,469.00(C) LIEN MMDDYYYY

    # pattern = re.compile(r'LTCC:.*\$(.*)\(C\).*LIEN')
    lien_match = re.search(r'LTCC:.*\$(.*)\(C\).*LIEN', last_lien)
    if lien_match:
        lien_amt = float(lien_match.group(1).replace(',', ''))
    else:
        # no pattern was found
        lien_amt = 0
    # lien_amt = float(re.findall(pattern, last_lien)[0].replace(',',''))

    print(lien_amt)

    return lien_amt

if __name__ == '__main__':
    get_lien_amount('abc')
