import PyPDF2
import camelot
import pandas as pd
import numpy as np
import re
import os
import glob
from tqdm import tqdm

def find_match_page(filename, pattern):
    """
    Find first page in PDF file where pattern string is found.

    Parameters:
        filename (string)
        pattern (string)

    
        page (int)
    """
    object = PyPDF2.PdfFileReader(filename)
    numPages = object.getNumPages()

    match_pages = []
    match_page = None
    for i in range(0, numPages):
        pageObj = object.getPage(i)
        text = pageObj.extractText()

        matches = re.search(pattern, text)
        if matches is not None:
            match_pages.append(i+1)

    if len(match_pages) != 0:
        if match_pages[0] > 5:
            match_page = match_pages[0]
        else:
            match_page = match_pages[1]
    return match_page

def extract_data(fileName, begin_page, end_page):
    """
    Extracts table from begin_page to end_page

    Parameters:
        fileName (str)
        begin_page (int)
        end_page (int)

    Returns:
        pandas.DataFrame object
    """
    dfs = []
    for page_num in range(begin_page, end_page+1):
        table = camelot.read_pdf(filename, pages=str(page_num), flavor='stream')
        #clean up columns
        df = table[0].df
#         if df.shape[1] != 5:
#             df = df.drop(columns=1)
#             df.columns = [0, 1, 2, 3, 4]
        dfs.append(df)

    return pd.concat(dfs, ignore_index=True)

def clean_data(df):
    """
    Processes and clean dataframe and casts appropriate data types

    Parameters:
        df (pandas.DataFrame object to clean)

    Returns:
        cleaned DataFrame
    """
    #rename columns
    num_cols = df.shape[1]

    if num_cols == 5:
        df.iloc[:, 1:num_cols] = (df.iloc[:, 1:num_cols].replace({'\$':'', '\(':'', '\)': '', '\,':''}, regex = True))
        if ('Annualized CR' in df.to_numpy()):
            df.columns = ['Category', 'Actuals', 'Annualized_CR', 'PresBud', 'PresBud_vs_Annualized_CR']
        if ('Enacted' in df.to_numpy()):
            df.columns = ['Category', 'Actuals', 'Enacted', 'PresBud', 'PresBud_vs_Enacted']
    elif num_cols == 6:
        df.iloc[:, 1:num_cols] = (df.iloc[:, 1:num_cols].replace({'\$':'', '\(':'', '\)': '', '\,':''}, regex = True))
        df.columns = ['Category', 'Enacted', 'Actuals', 'Annualized_CR', 'PresBud', 'PresBud_vs_Enacted']
    #replace empty strings with np.nan
    df.iloc[:, 1:num_cols] = (df.iloc[:, 1:num_cols].apply(pd.to_numeric, errors='coerce'))
    #drop na category
    df = df[-df['Category'].isna()].reset_index()
    return df.drop(columns='index')

def make_table(filename):
    """
    Calls in find_match_page, extract_data, clean_data with specific patterns to search within PDF

    Parameters:
        filename (PDF filename)

    Returns:
        Fully processed pandas.DataFrame from data in PDF table
    """
    begin_patterns = ["PROGRAM PROJECTS BY PROGRAM AREA", 'Projects by Program Area']
    end_pattern = "TOTAL, EPA"

    for pattern in begin_patterns:
        begin_page = find_match_page(filename, pattern)
        if begin_page is not None:
            break
        else:
            continue

    end_page = find_match_page(filename, end_pattern)
    df = extract_data(filename, begin_page, end_page)
    clean_df = clean_data(df)
    #add year column
    year = ''.join(re.findall('\d+', filename))
    clean_df.insert(0, 'Year', year)
    return clean_df

if __name__ == '__main__':
    final_dfs = []
    for filename in tqdm(sorted(glob.glob('EPA Budget in Brief/*.pdf'), reverse=True), desc='Extracting'):
        try:
            clean_df = make_table(filename)
            final_dfs.append(clean_df)
        except:
            pass
    final_df = pd.concat(final_dfs)
    final_df.to_csv('epa_budget.csv', index=False)
