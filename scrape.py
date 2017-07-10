from __future__ import print_function

__author__ = 'Adam King' # http://github.com/AdamKing11

import sys, os, csv, re, math, time

try:    
    from tqdm import tqdm
    from Queue import Queue
    import threading, requests
    from urllib2 import urlopen
    from bs4 import BeautifulSoup
except:
    print('Error loading required libraries.')
    sys.exit()

def read_bizlist(bizlist_file = 'Scrape Assignment Dataset - SearchExport.csv'):
    # read in the .csv file of Bussiness names
    biz_list = []
    with open(bizlist_file, 'r') as rf:
        reader = csv.DictReader(rf, delimiter='\t')
        for i, row in enumerate(reader):
            biz_list.append(row)
    return biz_list

def clean_links(links):
    # take search results, get the links 
    links = [l.get('href').lower() for l in links]
    links = [l[7:] for l in links if re.search(r'url\?q=http', l) and not re.search(r'webcache', l)]
    links = [re.sub(r'&sa=.*$', '', l) for l in links]
    return links


def google_query(q):
    # hit Google with a query for the bussiness name and extract the top 10 results
    address = 'https://www.google.com//search?q="%s"' % q
    time.sleep(13)
    r = requests.get(address)    
    soup = BeautifulSoup(r.text,  'lxml')
    links = clean_links(soup.find_all('a'))
    return links

def rank_links(biz_info, links, top_k = 3):
    # for each link, "score" it. for scoring, using simple co-occurence of webpage text/address
    # and info for the bussiness
    name = biz_info['Company Name'].lower()
    address = '%s %s' % (biz_info['Address Line 1'], biz_info['Address Line 2'])
    city = biz_info['City']
    country = biz_info['Country']
    
    scores = []
    links_and_scores = {}
    # loop through the links for the bussiness
    for l in links:
        s = 0
        # if the link itself contains part of the company name
        for name_word in name.rsplit(' '):
            if re.search(name_word, l): s += 2
            
        try:
            # download the webpage
            time.sleep(.7)
            r = requests.get(l, timeout=4)    
            soup = BeautifulSoup(r.text,  'lxml') 
            soup_str = str(soup)
            # if the webpage itself has parts of the address, it's relevant
            for feat in biz_info.values():
                if len(feat) > 0:   
                    if feat == name:
                        for name_word in name.rsplit(' '):
                            matches = re.findall(name_word, soup_str)
                            s += len(matches)
                    else:
                        matches = re.findall(feat.lower(), soup_str)   
                        s += len(matches)
        except:
            # if we get an error scraping, skip this link
            pass
        links_and_scores[l] = s
    
    # return the top k scoring links
    return sorted(links_and_scores, key=links_and_scores.get, reverse = True)[:top_k]

def chunk(l, n_chunk):
# take a list and return the list in n number of chunks
    if n_chunk > len(l):
        return l
    n = int(math.ceil(len(l) / float(n_chunk)))
    return [l[i:i+n] for i in range(0, len(l), n)]


def find_top_links(q, biz, outfile, fieldnames, top_k = 3):
    for b in biz:
        try:
            bname = b['Company Name'].lower()
            links = google_query(bname)
            ls = rank_links(b, links)
            b['URL 1'] = ls[0]
            b['URL 2'] = ls[1]
            b['URL 3'] = ls[2]
        except:
            pass
        # if we get less than 3 links, oh well...
        # had some problems with dict writer and multithreading so using this **ungly** code
        with open(output_file, 'a') as wf:
            row = '%s\t%s\t%s\t%s\t%s\t%s\t%s\t%s\n' % (b['Company Name'], b['Address Line 1'],
                b['Address Line 2'], b['City'], b['Country'], b['URL 1'], b['URL 2'], b['URL 3'])
            wf.write(row)
        lines_done = sum(1 for _ in open(outfile))
        
        print('%i -- Writing for "%s"' % (lines_done, b['Company Name']))
        if len(ls) == 0:
            print('\t\t**No links found for business!!**')
            sys.exit()
        if lines_done >= _nb_biz_to_scrape:
            print('Finished scraping %i businesses.' % _nb_biz_to_scrape)
            sys.exit()

_q = Queue()
_fieldnames = ['Company Name', 'Address Line 1', 'Address Line 2', 
            'City', 'Country', 'URL 1', 'URL 2', 'URL 3']
_nb_biz_to_scrape = sum(1 for _ in open('Scrape Assignment Dataset - SearchExport.csv')) - 1

if __name__ == '__main__':
    nb_thread = 4
    output_file = 'scraped.csv'

    # clear the file and write new headers
    with open(output_file, 'w') as wf:
        writer = csv.DictWriter(wf, fieldnames=_fieldnames, delimiter ='\t')
        writer.writeheader()

    # we'll open a couple threads and do the scraping in parallel
    print('reading in business names...')
    bz = read_bizlist()
    print('\tusing %i threads to scrape Google...' % nb_thread)
    bz_chunked = chunk(bz, nb_thread)  
    for i, b in enumerate(bz_chunked):
        thr = threading.Thread(target = find_top_links, args = (_q, b, output_file, _fieldnames))
        thr.daemon = True
        thr.start()
        # to offset queries so Google doesn't get mad....
        time.sleep(26)

    while True:
        _q.join()
        _q.get()
