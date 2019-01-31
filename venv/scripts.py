import requests
import pandas as pd
import time
s = requests.session()

y = 1547645097606
x = 0

url = (
            'https://www.bcb.gov.br/crsfn/proxyBuscaSP.asp?queryString=querytext=$plicsite:https://edicao-crsfn.bcb.gov.br/Documentos/%20$plic%26sourceid=$plic3aea6f46-8745-4ae8-98a7-75e485d13bdb$plic%26selectproperties=$plicfileName,Path,HitHighlightedSummary$plic%26refiners=$plicRefinableInt04(discretize=manual/1995/2000/2005/2010/2015),RefinableInt07(discretize=manual/100/150/200/250/300/350/400)$plic%26rowlimit=5%26refinementfilters=$plicAND(RefinableInt04:range(min,max),RefinableInt07:range(min,max))$plic%26startrow='
            + str(x)
            + '&_=' + str(y)
            )

r = s.get(url)
coleta = []

while len(r.json()['d']['query']['PrimaryQueryResult']['RelevantResults']['Table']['Rows']['results']) > 0:
    for a in range(0, 5):
        j = r.json()['d']['query']['PrimaryQueryResult']['RelevantResults']['Table']['Rows']['results'][a]
        res = {}
        for b in j['Cells']['results']:
            res[b['Key']] = b['Value']

        coleta += [res]
    x += 5
    y += 1

    url = (
            'https://www.bcb.gov.br/crsfn/proxyBuscaSP.asp?queryString=querytext=$plicsite:https://edicao-crsfn.bcb.gov.br/Documentos/%20$plic%26sourceid=$plic3aea6f46-8745-4ae8-98a7-75e485d13bdb$plic%26selectproperties=$plicfileName,Path,HitHighlightedSummary$plic%26refiners=$plicRefinableInt04(discretize=manual/1995/2000/2005/2010/2015),RefinableInt07(discretize=manual/100/150/200/250/300/350/400)$plic%26rowlimit=5%26refinementfilters=$plicAND(RefinableInt04:range(min,max),RefinableInt07:range(min,max))$plic%26startrow='
            + str(x)
            + '&_=' + str(x)
            )
    print(x)
    soneca = 1
    while True:
        try:
            r = s.get(url)
        except:
            time.sleep(soneca)
            soneca += 1
            print('>> Fail')

df = pd.DataFrame(coleta)
df.index.name = 'seq'
df.to_csv('D:\\Onedrive\\Projetos\\CVM\\Dataframes\\conselinho_completo.csv')



#baixa arquivos

import requests
import pandas as pd
import time

s = requests.session()
url = 'https://www.bcb.gov.br/crsfn/download.asp?'

df = pd.read_csv('D:\\Onedrive\\Projetos\\CVM\\Dataframes\\conselinho.csv', index_col='seq')
for linha in df[['fileName']].itertuples():

    payload = {'arquivo': requests.utils.quote(linha[1], encoding='cp1252')}
    r = s.get(url, params=payload)

    try:
        with open('D:\\Onedrive\\Projetos\\CVM\\Arquivos\\decisoes_conselinho\\' + linha[1], 'wb') as file:
            file.write(r.content)
        df.at[linha[0], 'arquivo_decisao'] = 'baixado'

    except Exception as e:
        df.at[linha[0], 'arquivo_decisao'] = str(e)

    print(linha[0])

df.to_csv('D:\\Onedrive\\Projetos\\CVM\\Dataframes\\conselinho.csv')

#extrai texto

import pandas as pd
import gc
from pdfminer.pdfparser import PDFParser, PDFDocument
from pdfminer.pdfinterp import PDFResourceManager, PDFPageInterpreter
from pdfminer.converter import PDFPageAggregator
from pdfminer.layout import LAParams, LTTextBox, LTTextLine


def pdf_2_tx(file):
    parser = PDFParser(file)
    doc = PDFDocument()
    parser.set_document(doc)
    doc.set_parser(parser)
    doc.initialize('')
    rsrcmgr = PDFResourceManager()
    laparams = LAParams()
    laparams.char_margin = 1.0
    laparams.word_margin = 1.0
    device = PDFPageAggregator(rsrcmgr, laparams=laparams)
    interpreter = PDFPageInterpreter(rsrcmgr, device)
    extracted_text = ''

    for page in doc.get_pages():
        interpreter.process_page(page)
        layout = device.get_result()
        for lt_obj in layout:
            if isinstance(lt_obj, LTTextBox) or isinstance(lt_obj, LTTextLine):
                extracted_text += lt_obj.get_text()

    return extracted_text


df = pd.read_csv('D:\\Onedrive\\Projetos\\CVM\\Dataframes\\conselinho.csv', index_col='seq')

for linha in df[['fileName']].itertuples():
    with open('D:\\Onedrive\\Projetos\\CVM\\Arquivos\\decisoes_conselinho\\' + linha[1], 'rb') as file:
        if linha[1][-4:] == '.pdf':
            df.at[linha[0], 'texto_arquivo'] = pdf_2_tx(file)

        else:
            df.at[linha[0], 'texto_arquivo'] = file.read()

    print(len(df), '//', linha[0])
    gc.collect()

df.to_csv('D:\\Onedrive\\Projetos\\CVM\\Dataframes\\conselinho.csv')



### pesquisa

import re
import numpy as np
import pandas as pd

df = pd.read_csv('C:\\Users\\ezequ\\Dropbox\\CVM\\dataframes\\html_sancionadores.csv', index_col='indexacao')


def busca(x):
    termos = ['infor.{,10}privi.*?\W', 'insider', 'infor.{,8}relev.*?\W', 'oscila.{,8}at.p.*?\W',
              '08/79', '31/84', '358/02']
    crit = [re.compile(t, re.I | re.DOTALL) for t in termos]

    res = []

    for c in crit:
        res += re.findall(c, x)

    return res


df['busca'] = df['html'].apply(busca) + df['subhtml'].apply(busca)

df['busca'] = df['busca'].apply(lambda x: x if len(x) > 0 else np.nan)

df = df[df['busca'].notnull()]
del df['html']
del df['subhtml']
del df['arquivo']

writer = pd.ExcelWriter(
    'C:\\Users\\ezequ\\Dropbox\\Núcleo de Estudos em Mercado e Investimentos\\Pesquisas\\insider_sancionadores.xlsx')
df.to_excel(writer, 'sancionadores')

writer.save()
writer.close()