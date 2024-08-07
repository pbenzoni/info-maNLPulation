# Combine the 3 csvs in the /data/comparison/ folder using 'content' column for dataset.csv, 'tweet' from the
# twitter dataset and 'content' from the facebook dataset.
from typing import List
import pandas as pd
import os

def combine_comparison_datasets(data_dir: str, output_file: str):
    # Load datasets
    test_df = pd.read_csv(os.path.join(data_dir, 'test.csv'))
    train_df = pd.read_csv(os.path.join(data_dir, 'train.csv'))
    dataset_df = pd.read_csv(os.path.join(data_dir, 'dataset.csv'))

    # Combine datasets as a single column
    combined_df = pd.DataFrame()
    combined_df['tweet_language'] = 'en'
    combined_df['tweet_text'] = pd.concat([test_df['tweet'], train_df['tweet'], dataset_df['content']], ignore_index=True)
    combined_df = combined_df[combined_df['tweet_text'].str.len() > 30]


    # Save combined dataset
    combined_df.to_csv(output_file, index=False)

    return combined_df

def apply_info_ops_filters(df: pd.DataFrame) -> pd.DataFrame:
    # Filter out retweets, i.e. is_retweet = FALSE
    #df = df[df['is_retweet'] == 'FALSE']

    # Filter out tweet_language where it is null or und or tweet length is less than 10
    df = df[df['tweet_language'].notnull()]
    df = df[df['tweet_language'] != 'und']
    df = df[df['tweet_text'].str.len() > 30]

    #filter out retweets
    df = df[df['is_retweet'] == False]

    return df

def combine_info_ops_datasets(data_dir: str, output_file_en: str,  output_file_all: str, output_file_slim: str):
    # Load datasets — all CSVs in the data directory into a dataframe. filter out tweet_language where it is not 'en'
    combined_df = pd.DataFrame()
    for file in os.listdir(data_dir):
        if file.endswith('.csv'):
            df = pd.read_csv(os.path.join(data_dir, file))
            df['source'] = file.split('_')[0]
            combined_df = pd.concat([combined_df, df], ignore_index=True)
            # include a column for the source of the data, whatever is before the first _ in the filename


    # Filter out retweets, i.e. is_retweet is false/0

    # filter out tweet_language where it is  null or und or tweet length is less than 10
    combined_df = apply_info_ops_filters(combined_df)
    
    # Display counts by language (tweet_language) 
    #print(combined_df['tweet_language'].value_counts())

    combined_df.to_csv(output_file_all, index=False)

    # slim to just the text and language columns and export to csv
    combined_df = combined_df[['tweet_text','tweet_language', 'source']]
    combined_df.to_csv(output_file_slim, index=False)

    combined_df = combined_df[combined_df['tweet_language'] == 'en']
    combined_df.to_csv(output_file_en, index=False)



    return combined_df

def make_temporal_info_ops_datasets(data_dir: str, output_file: str):
   # Load datasets — all CSVs in the data directory into a dataframe. filter out tweet_language where it is not 'en'
    combined_df = pd.DataFrame()
    for file in os.listdir(data_dir):
        # if the file ends _2016.csv or lower, then it is a 'early' dataset
        if file.endswith('_2016.csv') or file.endswith('_2015.csv') or file.endswith('_2014.csv') or file.endswith('_2013.csv') or file.endswith('_2012.csv') or file.endswith('_2011.csv'):
            df = pd.read_csv(os.path.join(data_dir, file))
            df['source'] = file.split('_')[0] + '_early'
            combined_df = pd.concat([combined_df, df], ignore_index=True)
        # if the file ends _2017.csv or higher, then it is a 'late' dataset
        elif file.endswith('_2017.csv') or file.endswith('_2018.csv') or file.endswith('_2019.csv') or file.endswith('_2020.csv') or file.endswith('_2021.csv'):
            df = pd.read_csv(os.path.join(data_dir, file))
            df['source'] = file.split('_')[0] + '_late'
            combined_df = pd.concat([combined_df, df], ignore_index=True)

    # Filter out retweets, i.e. is_retweet = FALSE  
    # combined_df = combined_df[combined_df['is_retweet'] == 'FALSE']

    # filter out tweet_language where it is  null or und or tweet length is less than 10
    combined_df = apply_info_ops_filters(combined_df)

    # slim to just the text and language columns and export to csv
    combined_df = combined_df[['tweet_text', 'tweet_language', 'source']]
    combined_df.to_csv(output_file, index=False)

    return combined_df

comparison = combine_comparison_datasets('C:\\Users\\benzo\\repo\\nlp-project\\data\\comparison', 'C:\\Users\\benzo\\repo\\nlp-project\\data\\combined_comparison_dataset.csv')
info_ops = combine_info_ops_datasets('C:\\Users\\benzo\\repo\\nlp-project\\data\\info_ops', 'C:\\Users\\benzo\\repo\\nlp-project\\data\\combined_info_ops_dataset_en.csv', 'C:\\Users\\benzo\\repo\\nlp-project\\data\\combined_info_ops_dataset_all.csv', 'C:\\Users\\benzo\\repo\\nlp-project\\data\\combined_info_ops_dataset_en_slim.csv',)
info_ops_temporal = make_temporal_info_ops_datasets('C:\\Users\\benzo\\repo\\nlp-project\\data\\info_ops', 'C:\\Users\\benzo\\repo\\nlp-project\\data\\combined_info_ops_dataset_temporal.csv')

comparison['source'] = 'comparison'
combined_df = pd.concat([comparison, info_ops], ignore_index=True)
#filter out sources with < 1000 tweets
combined_df = combined_df.groupby('source').filter(lambda x: len(x) > 1000)
combined_df.to_csv('C:\\Users\\benzo\\repo\\nlp-project\\data\\combined_dataset_detailed.csv', index=False)

#tagging source based on whcihc dataset it came from, then make a combined csv of the two datasets
comparison['source'] = 'comparison'
info_ops['source'] = 'info_ops'
combined_df = pd.concat([comparison, info_ops], ignore_index=True)
combined_df.to_csv('C:\\Users\\benzo\\repo\\nlp-project\\data\\combined_dataset.csv', index=False)

