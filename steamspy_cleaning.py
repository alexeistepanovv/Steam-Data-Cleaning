import pandas as pd
from datetime import datetime
from steam_cleaning import *

df_steamspy = pd.read_csv('./data/download/steamspy_app_data.csv')
#%%
df_steamspy
#%%
df_steamspy.isnull().sum()
#%%
df_steamspy['tags'] = df_steamspy['tags'].apply(parse_string)
#%%
def get_aprox_owners(owner_string):
    start, end = owner_string.split(' .. ')
    start = int(start.replace(',', ''))
    end = int(end.replace(',', ''))
    return (start + end) // 2
#%%
df_steamspy['approx_owners'] = df_steamspy['owners'].apply(get_aprox_owners)
#%%
float_to_int_cols = ['average_forever', 'average_2weeks', 'median_forever', 'median_2weeks', 'positive', 'negative']
df_steamspy[float_to_int_cols] = df_steamspy[float_to_int_cols].astype(int)
#%%
steam_cols = ['name', 'steam_appid', 'required_age', 'is_free',
              'minimum_req', 'recommended_req', 'developers', 'publishers',
              'date_str', 'windows', 'linux', 'mac', 'audio_text',
              'text_only', 'number_of_achievements']

steamspy_cols = ['appid', 'positive', 'negative', 'approx_owners',
                 'average_forever', 'average_2weeks', 'median_forever',
                 'median_2weeks', 'genre', 'ccu', 'tags']
df_steam = df_steam[steam_cols]
df_steamspy = df_steamspy[steamspy_cols]
df = df_steam.merge(df_steamspy, left_on='steam_appid', right_on='appid', how='left')
df.drop(columns=['appid'], inplace=True)

#%%
df = df.dropna()
float_to_int_cols = ['approx_owners', 'average_forever', 'average_2weeks',
                     'median_forever', 'median_2weeks', 'positive', 'negative']
df[float_to_int_cols] = df[float_to_int_cols].astype(int)
del steam_cols, steamspy_cols, float_to_int_cols
#%%
df_copy = df.copy()
#%%
df = df.drop_duplicates(subset='name')
#%%
import translators as ts
#%%
temp = df_copy['date_str'][80]
#%%
translated = ts.translate_text(query_text=temp, translator='bing', to_language='en')
#%%

def split_dataframe_by_chinese_symbols(df, columns_with_chinese):
    rows_with_chinese = []
    rows_without_chinese = []

    for index, row in df.iterrows():
        has_chinese = any(any('\u4e00' <= c <= '\u9fff' for c in str(row[column])) for column in columns_with_chinese)
        if has_chinese:
            rows_with_chinese.append(row)
        else:
            rows_without_chinese.append(row)

    df_with_chinese = pd.DataFrame(rows_with_chinese)
    df_without_chinese = pd.DataFrame(rows_without_chinese)

    return df_with_chinese, df_without_chinese
#%%
df_copy.info()
#%%
columns = ['name', 'date_str', 'text_only', 'audio_text', 'minimum_req', 'recommended_req']
df_with_chinese, df_without_chinese = split_dataframe_by_chinese_symbols(df_copy, columns_with_chinese=columns)

#%%
# from googletrans import Translator
# def translate_chinese(string):
#     translator = Translator()
#     if string:
#         translated = translator.translate(string, src='zh-cn', dest='en')
#         return translated
#     else:
#         return string
#%%
#%%
df['date_str'] = df['date_str'].apply(lambda x: 'unknown' if isinstance(x, float) else x)
#%%
def is_valid_date_format(date_str):
    valid_formats = [
        r'\d{1,2} [A-Za-z]{3}, \d{4}',   # e.g., '9 Jul, 2013'
        r'[A-Za-z]{3} \d{1,2}, \d{4}',   # e.g., 'Jul 9, 2013'
        r'[A-Za-z]{3} \d{4}'             # e.g., 'Jul 2013'
    ]

    for fmt in valid_formats:
        if re.fullmatch(fmt, date_str):
            return True
    return False

# Apply the function to the 'date_column' and create a new column 'is_valid_date'
invalid_dates_df = df[df['date_str'].apply(lambda date_str: not is_valid_date_format(date_str) if date_str else True)]
#%%
def extract_chinese_date(date_str):
    match = re.match(r'(\d{4}) 年 (\d{1,2}) 月 (\d{1,2}) 日', date_str)
    if match:
        year, month, day = match.groups()
        return f"{year}-{month.zfill(2)}-{day.zfill(2)}"
    else:
        return date_str
#%%
df['date_str'] = df['date_str'].apply(extract_chinese_date)
#%%
def parse_date(date_str):
    check_formats = ['%d %b %Y', '%b %Y', '%d %b, %Y', '%b %d, %Y']
    for fmt in check_formats:
        try:
            return datetime.strptime(date_str, fmt)
        except ValueError:
            pass
    return None
#%%
dtype_counts = df['date_str'].apply(type).value_counts()
df['date_str'] = df['date_str'].apply(lambda x: str(x) if isinstance(x, float) else x)
#%%
check_formats = ['%d %b %Y', '%b %Y', '%d %b, %Y', '%b %d, %Y']
df['release_date'] = df['date_str'].apply(parse_date)
#%%
rec_patterns = {
    'req_os': r'OS:\s+(.*?)Processor:',
    'req_processor': r'Processor:\s+(.*?)Memory:',
    'rec_memory': r'Memory:\s+(.*?)Graphics:',
    'rec_graphics': r'Graphics:\s+(.*?)DirectX:',
    'rec_directX': r'DirectX:\s+(.*?)Network:',
    'rec_network': r'Network:\s+(.*?)Storage:',
    'rec_storage': r'Storage:\s+(.*?)Additional Notes:'
}
min_patterns = {
    'min_os': r'OS:\s+(.*?)Processor:',
    'min_processor': r'Processor:\s+(.*?)Memory:',
    'min_memory': r'Memory:\s+(.*?)Graphics:',
    'min_graphics': r'Graphics:\s+(.*?)DirectX:',
    'min_directX': r'DirectX:\s+(.*?)Network:',
    'min_network': r'Network:\s+(.*?)Storage:',
    'min_storage': r'Storage:\s+(.*?)Additional Notes:'
}
#%%
def extract_requirements(requirements_string, patterns):
    # Initialize empty dictionaries to store extracted data
    extracted_data = {}
    # Iterate through patterns and extract data
    for key, pattern in patterns.items():
        match = re.search(pattern, requirements_string)
        if match:
            extracted_data[key] = match.group(1).strip()
    return extracted_data
#%%
# Initialize empty lists for storing extracted data
rec_data_list = []
min_data_list = []
unmatched_data_list = []
#%%
# Iterate through rows and extract data
for index, row in df.iterrows():
    requirements_string = row['minimum_req']
    extracted_min_data = extract_requirements(requirements_string, min_patterns)
    if extracted_min_data:
        min_data_list.append(extracted_min_data)
    if not extracted_min_data:
        unmatched_data_list.append({'index': index, 'requirements': requirements_string})

# Convert extracted data lists to DataFrames
rec_df = pd.DataFrame(rec_data_list)
min_df = pd.DataFrame(min_data_list)
unmatched_df = pd.DataFrame(unmatched_data_list)
#%%
df.to_csv('data/steam_data_merged_cleaned.csv', index=False)