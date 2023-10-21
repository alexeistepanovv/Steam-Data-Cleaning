# importing
import pandas as pd
import re
import ast
import json
# %%
# Load scraped steam data. Check
df_steam = pd.read_csv('./data/download/steam_app_data.csv')
df_steam.head(6)
# %%
print(df_steam.info())
print(df_steam.isnull().sum())
# %%
# Lot of columns with same small number of missing values.
# Dropping missing values from columns with less that 1% of missing data
# This will handle several columns with missing values with a loss of nearly 150 games
def drop_na_rows_less_than_threshold(target_df, threshold=0.01):
    """
    Drop columns from the DataFrame where the percentage of missing data is more than the specified threshold.

    Parameters:
        target_df (pandas.DataFrame): The input DataFrame.
        threshold (float, optional): The threshold for the percentage of missing data (default is 0.5, i.e., 50%).

    Returns:
        pandas.DataFrame: The DataFrame with columns dropped.
    """
    cols = target_df.columns
    n_rows = round(len(target_df) * threshold)

    for arg in cols:
        if target_df[arg].isna().sum() < n_rows:
            dropped_df = target_df.dropna(subset=[arg])
    return dropped_df


# %%
# Dropping columns with more that 50% of missing data
# Moreover these columns are not so useful for further analysis
def drop_na_columns_more_than_threshold(target_df, threshold=0.5):
    """
    Drop rows from the DataFrame where the percentage of missing data is less than the specified threshold.

    Parameters:
        target_df (pandas.DataFrame): The input DataFrame.
        threshold (float, optional): The threshold for the percentage of missing data (default is 0.01, i.e., 1%).

    Returns:
        pandas.DataFrame: The DataFrame with rows dropped.
    """
    num_rows = len(target_df)
    threshold_count = num_rows * threshold
    drop_cols = [col for col in target_df.columns if df_steam[col].isna().sum() > threshold_count]
    df_steam_dropped = target_df.drop(columns=drop_cols)
    return df_steam_dropped
# %%
# Dropping values and columns
df_steam = drop_na_rows_less_than_threshold(df_steam)
df_steam = drop_na_columns_more_than_threshold(df_steam)
# %%
# Replacing missing data with "unknown" string value
columns = ['website', 'price_overview', 'packages', 'categories', 'movies', 'achievements']
for column in columns:
    df_steam[column] = df_steam[column].replace({pd.NA: 'unknown'})
del columns
# Dropping useless data
df_steam = df_steam.drop(columns=['screenshots', 'movies', 'support_info', 'background', 'content_descriptors'])
# %%
# no NA data from here
# note that there are still a lot of missing data, we just replaced NA with 'unknown'
# further analysis will help with understanding of missing values
print(df_steam.isnull().sum())
# %%
# data is full of html tags, which should be cleaned
def remove_html(string):
    """
    Removes HTML tags and backslashes from the input string.
    Parameters:
        string (str): The input string to be cleaned from HTML tags and backslashes.
    Returns:
        str: The cleaned string with HTML tags and backslashes removed.
    """
    regex = re.compile(r'<[^>]+>')
    cleaned = regex.sub('', string)
    cleaned = cleaned.replace('\\', '')
    return cleaned

#%%
# # Filter rows where 'supported_languages' column contains floats
# float_rows = df_steam[df_steam['supported_languages'].apply(lambda x: isinstance(x, float))]
# # Get the indices of the float rows
# float_row_indices = float_rows.index
# # Drop the float rows from the original DataFrame
# df_steam = df_steam.drop(float_row_indices)
# #%%
# # Filter rows where 'supported_languages' column contains floats
# float_rows = df_steam[df_steam['short_description'].apply(lambda x: isinstance(x, float))]
#
# # Print the float rows
# print(float_rows)
#%%
columns_to_transform = ['supported_languages', 'detailed_description', 'about_the_game', 'short_description']
# Loop through each column and convert float values to strings
for col in columns_to_transform:
    df_steam[col] = df_steam[col].astype(str)
#%%
df_steam['supported_languages'] = df_steam['supported_languages'].apply(remove_html)
# %%
# creating reviews DataFrame cleaned from tags
review_df_steam_cols = ['steam_appid', 'detailed_description', 'about_the_game', 'short_description']
review_cols = ['detailed_description', 'about_the_game', 'short_description']
review_df_steam = df_steam[review_df_steam_cols]
df_steam = df_steam.drop(columns=['detailed_description', 'about_the_game', 'short_description'])

for col in review_cols:
    review_df_steam[col] = review_df_steam[col].apply(remove_html)
del review_cols, review_df_steam_cols, col, column
# %%
# Dropping requirements for Linux and Mac as there are not so many of them, we still have PC data
df_steam = df_steam.drop(columns=['linux_requirements', 'mac_requirements'])
# %%
# Cleaning tags in languages and requirements
df_steam['supported_languages'] = df_steam['supported_languages'].apply(remove_html)
df_steam['pc_requirements'] = df_steam['pc_requirements'].apply(remove_html)
# %%
# Price column appear to be dictionary in string format
# Extracting currency and initial price variables
def extract_price_info(price_string):
    """
    Extracts currency and initial price information from the given price string.
    This function takes a price string containing price information in a specific format and extracts the currency
    and initial price from it.
    Parameters:
        price_string (str): The price information in string format. It should be in the format of a dictionary with keys
                            'currency' and 'initial', representing the currency code and the initial price value.
    Returns:
        tuple: A tuple containing the currency and initial price information. If the price_string is 'unknown', the
               function returns ('unknown', 'unknown').
    """
    if price_string != 'unknown':
        price_dict = eval(price_string)
        currency = price_dict.get('currency')
        initial_price = price_dict.get('initial')
        return currency, initial_price
    else:
        currency = 'unknown'
        initial_price = 'unknown'
        return currency, initial_price


# Apply the 'extract_price_info' function to create new columns
df_steam[['currency', 'initial_price']] = df_steam['price_overview'].apply(extract_price_info).apply(pd.Series)
# %%
# There are two main types of NAs in price overview columns:
# True missing data, when the game is not free
condition = (df_steam['is_free'] == True) & (df_steam['initial_price'] == 'unknown') & (
        df_steam['currency'] == 'unknown')
df_steam.loc[condition, ['initial_price', 'currency']] = 'free'
rows_changed = condition.sum()
print("Number of rows changed:", rows_changed)  # ~5k games appear to be free
del condition, rows_changed
# Around 3k rows are missing by some data acquisition reason
print(len(df_steam[df_steam['initial_price'] == 'unknown']))
# Now we can delete 'price_overview' column
df_steam = df_steam.drop('price_overview', axis=1)


#%%
# Splitting platform dictionary into binary columns for each platform
def extract_platform_info(df_steam, target_col):
    def get_platform_info(platform_str):
        platform_dict = ast.literal_eval(platform_str)
        return {
            'windows': platform_dict.get('windows', False),
            'linux': platform_dict.get('linux', False),
            'mac': platform_dict.get('mac', False)
        }

    df_steam['platforms'] = df_steam[target_col].apply(get_platform_info)
    df_steam = pd.concat([df_steam, df_steam['platforms'].apply(pd.Series)], axis=1)
    df_steam.drop(columns=['platforms'], inplace=True)
    return df_steam


# %%
df_steam = extract_platform_info(df_steam, 'platforms')
#%%
columns_to_transform = ['developers', 'publishers']
# Loop through each column and convert float values to strings
for col in columns_to_transform:
    df_steam[col] = df_steam[col].astype(str)

# %%
# creating a list of developers and publishers for easier further access
def string_to_list_cleaning(df_steam, target_col):
    df_steam[target_col] = df_steam[target_col].apply(lambda x: [element.strip("[ '']") for element in x.split(",")])
    return df_steam
# %%
df_steam = string_to_list_cleaning(df_steam, 'developers')
df_steam = string_to_list_cleaning(df_steam, 'publishers')


# %%
# Processing languages column and creating separate columns for audio text support and text only support
# '*' connected to language is an indicator of audio support of this specific language
def process_languages(language_string):
    if isinstance(language_string, str):
        language_list = language_string.split(',')
    elif isinstance(language_string, list):
        language_list = language_string
    else:
        raise ValueError('Use string of list format')
    last_language = language_list[-1]
    if last_language.endswith('languages with full audio support'):
        language_list[-1] = last_language.rstrip('languages with full audio support')
    return language_list


def create_language_columns(language_list):
    full_audio_list = []
    full_text_list = []
    for lang in language_list:
        if lang.endswith('*'):
            full_audio_list.append(lang)
        else:
            full_text_list.append(lang)
    return [full_audio_list, full_text_list]


# %%
df_steam['supported_languages'] = df_steam['supported_languages'].apply(process_languages)
df_steam['audio_text'] = df_steam['supported_languages'].apply(create_language_columns).str[0]
df_steam['text_only'] = df_steam['supported_languages'].apply(create_language_columns).str[1]
df_steam.drop(columns=['supported_languages'], inplace=True)
# %%
df_steam['audio_text'] = df_steam['audio_text'].apply(lambda x: [lang.rstrip('*').strip() for lang in x])
df_steam['text_only'] = df_steam['text_only'].apply(lambda x: [lang.strip() for lang in x])
# %%
df_steam['package_groups'] = df_steam['package_groups'].apply(ast.literal_eval)


# %%
# Decided to create a separate dataframe for distinct packages with 'appid' as foreign key
def extract_package_info(df_steam):
    appids = []
    package_id = []
    names = []
    package_names = []
    is_free_list = []
    prices = []

    # Utility function to extract package info from each row
    def process_package_groups(package_groups, appid, name):
        if package_groups:
            for package in package_groups[0]['subs']:
                appids.append(appid)
                names.append(name)
                package_id.append(package['packageid'])
                package_names.append(package['option_text'])
                is_free_list.append(package['is_free_license'])
                prices.append(package['price_in_cents_with_discount'] / 100)  # Convert cents to dollars
        else:
            # If there are no package groups, add None or NaN for the relevant fields
            appids.append(appid)
            names.append(name)
            package_id.append(None)
            package_names.append(None)
            is_free_list.append(None)
            prices.append(None)
    # Iterate through the rows of the original DataFrame
    for index, row in df_steam.iterrows():
        package_groups = row['package_groups']
        process_package_groups(package_groups, row['steam_appid'], row['name'])
    # Create a new DataFrame with the extracted data
    package_df_steam = pd.DataFrame({
        'package_id': package_id,
        'appid': appids,
        'name': names,
        'package_name': package_names,
        'is_free': is_free_list,
        'price': prices
    })
    return package_df_steam


# %%
# Leaving only packages IDs in main DataFrame, new DataFrame created
package_df_steam = extract_package_info(df_steam)
df_steam = df_steam.drop(columns='package_groups')
# %%
# Changing broken datatypes
package_df_steam = package_df_steam.dropna()
package_format = {
    'package_id': int,
    'price': float
}
package_df_steam = package_df_steam.astype(package_format)
# %%
package_df_steam['package_name'] = package_df_steam['package_name'].apply(remove_html)


# %%
# Some cleaning of newly created columns
def remove_from_last_hyphen(text):
    reversed_text = text[::-1]
    split_result = reversed_text.split('-', 1)
    if len(split_result) > 1:
        return split_result[1][::-1].strip()
    else:
        return text


# Apply the function to the 'package_name' column
package_df_steam['package_name'] = package_df_steam['package_name'].apply(remove_from_last_hyphen)


# %%
# As we have lists of dictionaries, and some nested structures, decided to use JSON library
def parse_string(string):
    string = string.replace("'", '"')
    try:
        return json.loads(string)
    except (json.JSONDecodeError, TypeError):
        return []
#%%
df_steam['achievements'] = df_steam['achievements'].apply(parse_string)
#%%
#%% Extracting number of achievements
def parse_achievements(achievement_dict):
    if achievement_dict:
        total = achievement_dict['total']
    else:
        total = 'unknown'
    return total
#%%
df_steam['number_of_achievements'] = df_steam['achievements'].apply(parse_achievements)
df_steam = df_steam.drop(columns='achievements')
#%%
df_steam['release_date'] = df_steam['release_date'].apply(ast.literal_eval)
#%%
# Parsing date dictionary, two new columns created
def split_dict(row):
    return pd.Series([row['coming_soon'], row['date']])
#%%
df_steam[['coming_soon', 'date_str']] = df_steam['release_date'].apply(split_dict)
df_steam.drop(columns=['release_date'], inplace=True)
df_steam['date_str'] = df_steam['date_str'].apply(str.lower)
#%%
df_steam['pc_requirements'] = df_steam['pc_requirements'].apply(parse_string)
#%%
temp = df_steam['pc_requirements'][114]
#%%
def parse_requirements(requirements_dict):
    minimum = []
    recomm = []

    if requirements_dict:
        if 'minimum' in requirements_dict:
            minimum.append(requirements_dict['minimum'])
        if 'recommended' in requirements_dict:
            recomm.append(requirements_dict['recommended'])

    return minimum, recomm
#%%
# Apply the parse_requirements function using the apply method
df_steam[['minimum_req', 'recommended_req']] = df_steam['pc_requirements'].apply(parse_requirements).apply(pd.Series)

# Drop the original pc_requirements column
df_steam.drop(columns=['pc_requirements'], inplace=True)

#%%
